// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/tools/compatibility/gen-cpp2/SerdePayload_types.h"

namespace facebook::nimble::tools::compatibility {

SerializationVersion parseFormat(std::string_view format);

std::string_view formatName(SerializationVersion format);

class Payload {
 public:
  Payload() = default;
  explicit Payload(SerdePayload envelope);

  SerdePayload& envelope();
  const SerdePayload& envelope() const;

  SerializationVersion format() const;
  void writeToFile(const std::string& path) const;

  static Payload readFromFile(const std::string& path);

 private:
  SerdePayload envelope_;
};

enum class Status {
  Passed,
  Incompatible,
  TestDataError,
  NotExercised,
  InfraError,
};

struct Result {
  std::string schemaProfile;
  std::string writerProfile;
  std::string readCaseId;
  std::map<std::string, std::string> resolvedWriterOptions;
  std::map<std::string, std::string> featureEncodingOverrides;
  Status status{Status::InfraError};
  std::string failureSummary;
};

class Report {
 public:
  int32_t reportVersion{0};
  std::string catalogVersion;
  std::string writerCommit;
  std::string readerCommit;
  std::string format;
  std::string checkKind;
  std::vector<Result> results;

  void writeToFile(const std::string& path) const;
  std::string summary() const;
};

std::string_view statusName(Status status);

} // namespace facebook::nimble::tools::compatibility
