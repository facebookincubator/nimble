// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "common/base/BuildInfo.h"
#include "common/network/NetworkUtil.h"

namespace facebook::alpha::detail {

std::unordered_map<std::string, std::string> defaultMetadata() {
  std::unordered_map<std::string, std::string> metadata{
      {"hostname", network::NetworkUtil::getLocalHost(true)}};

  auto buildRevision = BuildInfo::getBuildRevision();
  if (buildRevision && buildRevision[0] != '\0') {
    metadata.insert({"build.revision", buildRevision});
  }

  auto packageName = BuildInfo::getBuildPackageName();
  auto packageVersion = BuildInfo::getBuildPackageVersion();
  if (packageName && packageName[0] != '\0') {
    std::string name{packageName};
    if (packageVersion && packageVersion[0] != '\0') {
      name += ":";
      name += packageVersion;
    }
    metadata.insert({"build.package", std::move(name)});
  }

  return metadata;
}

} // namespace facebook::alpha::detail
