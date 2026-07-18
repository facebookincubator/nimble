/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include "dwio/nimble/index/ClusterIndexFactory.h"

#include <mutex>
#include <optional>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/ClusterIndex.h"
#include "dwio/nimble/index/ClusterIndexWriter.h"
#include "folly/container/F14Map.h"

namespace facebook::nimble::index {

namespace {

using FactoryMap =
    folly::F14FastMap<std::string, std::shared_ptr<const ClusterIndexFactory>>;

FactoryMap& factories() {
  static auto* map = new FactoryMap();
  return *map;
}

std::mutex& factoriesMutex() {
  static auto* mutex = new std::mutex();
  return *mutex;
}

class NimbleClusterIndexFactory final : public ClusterIndexFactory {
 public:
  std::string_view name() const override {
    return kClusterIndexName;
  }

  std::unique_ptr<IndexWriter> createWriter(
      const ClusterIndexConfig& config,
      const velox::TypePtr& inputType,
      velox::memory::MemoryPool* pool) const override {
    return ClusterIndexWriter::create(
        std::optional<ClusterIndexConfig>{config}, inputType, pool);
  }

  std::unique_ptr<IndexKeyEncoder> createKeyEncoder(
      const std::vector<std::string>& columns,
      const velox::RowTypePtr& inputType,
      const std::vector<SortOrder>& sortOrders,
      velox::memory::MemoryPool* pool) const override {
    return createNimbleIndexKeyEncoder(columns, inputType, sortOrders, pool);
  }

  std::unique_ptr<ClusterIndex> createReader(
      Section rootSection,
      velox::memory::MemoryPool* pool,
      const IndexLookup::Options& options) const override {
    return ClusterIndex::create(std::move(rootSection), pool, options);
  }
};

void ensureBuiltInFactoriesRegistered() {
  static const bool registered = [] {
    registerClusterIndexFactory(
        std::make_shared<const NimbleClusterIndexFactory>());
    return true;
  }();
  (void)registered;
}

} // namespace

void registerClusterIndexFactory(
    std::shared_ptr<const ClusterIndexFactory> factory) {
  NIMBLE_CHECK_NOT_NULL(factory);
  const auto name = std::string{factory->name()};
  NIMBLE_CHECK(!name.empty(), "Cluster index factory name cannot be empty");
  std::lock_guard lock(factoriesMutex());
  auto [_, inserted] = factories().emplace(name, std::move(factory));
  NIMBLE_CHECK(inserted, "Cluster index factory already registered: {}", name);
}

const ClusterIndexFactory& clusterIndexFactory(std::string_view name) {
  ensureBuiltInFactoriesRegistered();
  std::lock_guard lock(factoriesMutex());
  auto it = factories().find(name);
  NIMBLE_CHECK(
      it != factories().end(), "Unknown cluster index factory: {}", name);
  return *it->second;
}

} // namespace facebook::nimble::index
