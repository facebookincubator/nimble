/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "common/strings/UUID.h"
#include "dsi/logger/configs/XldbAlphaLoggerConfig/Logger.h"
#include "dwio/nimble/common/MetricsLogger.h"

namespace facebook::nimble {

class DefaultMetricsLogger : public MetricsLogger {
 public:
  DefaultMetricsLogger(
      const std::string& ns,
      const std::string& table,
      const std::string& hostName,
      const std::string& clientId,
      std::string queryId = strings::generateUUID());

  void logException(std::string_view operation, const std::string& errorMessage)
      const override;

  void logStripeLoad(const StripeLoadMetrics& metrics) const override;
  void logStripeFlush(const StripeFlushMetrics& metrics) const override;
  void logFileClose(const FileCloseMetrics& metrics) const override;
  void logZstrongContext(const std::string& context) const override;

 private:
  void populateAccessorInfo(logger::XldbAlphaLogger& log) const;

  std::string ns_;
  std::string table_;
  std::string hostName_;
  std::string clientId_;
  std::string queryId_;
};

} // namespace facebook::nimble
