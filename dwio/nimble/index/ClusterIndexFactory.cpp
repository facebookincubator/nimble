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

#include <optional>

#include "dwio/nimble/index/ClusterIndex.h"
#include "dwio/nimble/index/ClusterIndexWriter.h"
#include "dwio/nimble/index/IndexFactoryRegistry.h"

namespace facebook::nimble::index {

namespace {

IndexFactoryRegistry<ClusterIndexFactory>& factoryRegistry() {
  static auto* registry =
      new IndexFactoryRegistry<ClusterIndexFactory>{IndexFamily::Cluster};
  return *registry;
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
    factoryRegistry().registerFactory(
        std::make_shared<const NimbleClusterIndexFactory>());
    return true;
  }();
  (void)registered;
}

} // namespace

void registerClusterIndexFactory(
    std::shared_ptr<const ClusterIndexFactory> factory) {
  ensureBuiltInFactoriesRegistered();
  factoryRegistry().registerFactory(std::move(factory));
}

const ClusterIndexFactory& clusterIndexFactory(std::string_view name) {
  ensureBuiltInFactoriesRegistered();
  return factoryRegistry().get(name);
}

} // namespace facebook::nimble::index
