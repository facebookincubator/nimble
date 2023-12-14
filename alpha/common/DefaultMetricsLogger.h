// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "common/strings/UUID.h"
#include "dsi/logger/configs/XldbAlphaLoggerConfig/Logger.h"
#include "dwio/alpha/common/MetricsLogger.h"

namespace facebook::alpha {

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

} // namespace facebook::alpha
