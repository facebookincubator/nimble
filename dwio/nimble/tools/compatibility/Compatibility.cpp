// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/tools/compatibility/Compatibility.h"

#include <fstream>
#include <iterator>
#include <utility>

#include <fmt/format.h>
#include <folly/json/json.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <velox/common/base/Exceptions.h>

#include "dwio/nimble/serializer/SerializationHeader.h"

namespace facebook::nimble::tools::compatibility {
namespace {

SerializationVersion payloadVersion(std::string_view payload) {
  VELOX_CHECK(!payload.empty(), "Compatibility payload is empty");
  const auto* position = payload.data();
  return nimble::serde::readSerializationHeader(
             position, payload.data() + payload.size(), /*hasHeader=*/true)
      .version;
}

void validateFormat(SerializationVersion format) {
  switch (format) {
    case SerializationVersion::kLegacyCompact:
    case SerializationVersion::kSerialization:
    case SerializationVersion::kProjection:
    case SerializationVersion::kTablet:
      return;
    case SerializationVersion::kLegacy:
    case SerializationVersion::kLegacySerialization:
      VELOX_UNSUPPORTED(
          "Unsupported compatibility serialization version: {}",
          nimble::toString(format));
  }
  VELOX_UNREACHABLE("Unknown serialization version");
}

folly::dynamic resultToJson(const Result& result) {
  folly::dynamic writerOptions = folly::dynamic::object();
  for (const auto& [name, value] : result.resolvedWriterOptions) {
    writerOptions[name] = value;
  }
  folly::dynamic featureEncodingOverrides = folly::dynamic::object();
  for (const auto& [subfield, encoding] : result.featureEncodingOverrides) {
    featureEncodingOverrides[subfield] = encoding;
  }
  return folly::dynamic::object("schema_profile", result.schemaProfile)(
      "writer_profile", result.writerProfile)(
      "read_case_id", result.readCaseId)(
      "resolved_writer_options", std::move(writerOptions))(
      "feature_encoding_overrides", std::move(featureEncodingOverrides))(
      "status", statusName(result.status))(
      "failure_summary", result.failureSummary);
}

uint8_t statusSeverity(Status status) {
  switch (status) {
    case Status::Passed:
      return 0;
    case Status::NotExercised:
      return 1;
    case Status::Incompatible:
      return 2;
    case Status::TestDataError:
      return 3;
    case Status::InfraError:
      return 4;
  }
  VELOX_UNREACHABLE("Unsupported validation status");
}

} // namespace

SerializationVersion parseFormat(std::string_view format) {
  if (format == "legacy_compact") {
    return SerializationVersion::kLegacyCompact;
  }
  if (format == "serialization") {
    return SerializationVersion::kSerialization;
  }
  if (format == "projection") {
    return SerializationVersion::kProjection;
  }
  if (format == "tablet") {
    return SerializationVersion::kTablet;
  }
  VELOX_USER_FAIL("Unsupported compatibility format: {}", format);
}

std::string_view formatName(SerializationVersion format) {
  validateFormat(format);
  switch (format) {
    case SerializationVersion::kLegacyCompact:
      return "legacy_compact";
    case SerializationVersion::kSerialization:
      return "serialization";
    case SerializationVersion::kProjection:
      return "projection";
    case SerializationVersion::kTablet:
      return "tablet";
    case SerializationVersion::kLegacy:
    case SerializationVersion::kLegacySerialization:
      VELOX_UNREACHABLE("Unsupported compatibility serialization version");
  }
  VELOX_UNREACHABLE("Unknown serialization version");
}

Payload::Payload(SerdePayload envelope) : envelope_{std::move(envelope)} {}

SerdePayload& Payload::envelope() {
  return envelope_;
}

const SerdePayload& Payload::envelope() const {
  return envelope_;
}

SerializationVersion Payload::format() const {
  VELOX_CHECK(!envelope_.payloads()->empty(), "No payload rows were produced");
  const auto format = payloadVersion(envelope_.payloads()->front());
  validateFormat(format);
  for (const auto& rowPayload : *envelope_.payloads()) {
    VELOX_CHECK_EQ(
        payloadVersion(rowPayload),
        format,
        "Payload rows have inconsistent serialization versions");
  }
  return format;
}

void Payload::writeToFile(const std::string& path) const {
  const auto data =
      apache::thrift::CompactSerializer::serialize<std::string>(envelope_);
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  VELOX_CHECK(output, "Failed to open output file: {}", path);
  output.write(data.data(), data.size());
  VELOX_CHECK(output, "Failed to write output file: {}", path);
}

Payload Payload::readFromFile(const std::string& path) {
  std::ifstream input{path, std::ios::binary};
  VELOX_CHECK(input, "Failed to open input file: {}", path);
  const std::string data{
      std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
  SerdePayload envelope;
  apache::thrift::CompactSerializer::deserialize(data, envelope);
  return Payload{std::move(envelope)};
}

std::string_view statusName(Status status) {
  switch (status) {
    case Status::Passed:
      return "PASSED";
    case Status::Incompatible:
      return "INCOMPATIBLE";
    case Status::TestDataError:
      return "TEST_DATA_ERROR";
    case Status::NotExercised:
      return "NOT_EXERCISED";
    case Status::InfraError:
      return "INFRA_ERROR";
  }
  VELOX_UNREACHABLE("Unsupported validation status");
}

void Report::writeToFile(const std::string& path) const {
  folly::dynamic jsonResults = folly::dynamic::array();
  folly::dynamic safeProfiles = folly::dynamic::array();
  folly::dynamic unsafeProfiles = folly::dynamic::array();
  folly::dynamic notExercisedProfiles = folly::dynamic::array();
  auto overallStatus = Status::Passed;
  for (const auto& result : results) {
    jsonResults.push_back(resultToJson(result));
    const auto profile =
        fmt::format("{}/{}", result.writerProfile, result.readCaseId);
    switch (result.status) {
      case Status::Passed:
        safeProfiles.push_back(profile);
        break;
      case Status::NotExercised:
        notExercisedProfiles.push_back(profile);
        break;
      case Status::Incompatible:
      case Status::TestDataError:
      case Status::InfraError:
        unsafeProfiles.push_back(profile);
        break;
    }
    if (statusSeverity(result.status) > statusSeverity(overallStatus)) {
      overallStatus = result.status;
    }
  }
  const folly::dynamic json =
      folly::dynamic::object("report_version", reportVersion)(
          "catalog_version", catalogVersion)("writer_commit", writerCommit)(
          "reader_commit", readerCommit)("format", format)(
          "check_kind", checkKind)("overall_status", statusName(overallStatus))(
          "results", std::move(jsonResults))(
          "safe_profiles", std::move(safeProfiles))(
          "unsafe_profiles", std::move(unsafeProfiles))(
          "not_exercised_profiles", std::move(notExercisedProfiles));
  folly::json::serialization_opts options;
  options.pretty_formatting = true;
  const auto data = folly::json::serialize(json, options);
  std::ofstream output{path, std::ios::trunc};
  VELOX_CHECK(output, "Failed to open report file: {}", path);
  output << data << '\n';
  VELOX_CHECK(output, "Failed to write report file: {}", path);
}

std::string Report::summary() const {
  std::string output = fmt::format(
      "Nimble compatibility: writer={} reader={} format={} check={}\n",
      writerCommit,
      readerCommit,
      format,
      checkKind);
  for (const auto& result : results) {
    output += fmt::format(
        "  {} / {} / {}: {}{}\n",
        result.schemaProfile,
        result.writerProfile,
        result.readCaseId,
        statusName(result.status),
        result.failureSummary.empty()
            ? ""
            : fmt::format(" - {}", result.failureSummary));
  }
  return output;
}

} // namespace facebook::nimble::tools::compatibility
