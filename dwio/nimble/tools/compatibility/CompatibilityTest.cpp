// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/nimble/tools/compatibility/Compatibility.h"

#include <filesystem>
#include <fstream>

#include <folly/json/json.h>
#include <gtest/gtest.h>

#include "dwio/nimble/serializer/SerializationHeader.h"

namespace facebook::nimble::tools::compatibility {
namespace {

TEST(CompatibilityTest, UsesSerializationVersionAsFormat) {
  EXPECT_EQ(
      parseFormat("legacy_compact"), SerializationVersion::kLegacyCompact);
  EXPECT_EQ(parseFormat("serialization"), SerializationVersion::kSerialization);
  EXPECT_EQ(parseFormat("projection"), SerializationVersion::kProjection);
  EXPECT_EQ(parseFormat("tablet"), SerializationVersion::kTablet);
  EXPECT_THROW(parseFormat("legacy"), velox::VeloxException);
}

TEST(CompatibilityTest, DetectsLegacyCompactPayload) {
  std::string rowPayload;
  serde::writeLegacySerializationHeader(
      rowPayload, SerializationVersion::kLegacyCompact, /*rowCount=*/1);
  SerdePayload envelope;
  *envelope.payloads() = {std::move(rowPayload)};

  const Payload payload{std::move(envelope)};

  EXPECT_EQ(payload.format(), SerializationVersion::kLegacyCompact);
  EXPECT_EQ(formatName(payload.format()), "legacy_compact");
}

TEST(CompatibilityTest, ReportDoesNotMaskFailureWithNotExercised) {
  const Report report{
      .reportVersion = 1,
      .catalogVersion = "1",
      .writerCommit = "writer",
      .readerCommit = "reader",
      .format = "legacy_compact",
      .checkKind = "candidate_current",
      .results =
          {{.writerProfile = "unsafe",
            .readCaseId = "full",
            .status = Status::Incompatible},
           {.writerProfile = "not_run",
            .readCaseId = "full",
            .status = Status::NotExercised}},
  };
  const auto path =
      std::filesystem::temp_directory_path() / "nimble_compatibility_test.json";

  report.writeToFile(path.string());

  std::ifstream input{path};
  const std::string data{
      std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
  const auto json = folly::parseJson(data);
  EXPECT_EQ(json["overall_status"], "INCOMPATIBLE");
  std::filesystem::remove(path);
}

} // namespace
} // namespace facebook::nimble::tools::compatibility
