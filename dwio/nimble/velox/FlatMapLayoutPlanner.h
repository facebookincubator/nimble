// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/tablet/Tablet.h"
#include "dwio/nimble/velox/SchemaBuilder.h"

namespace facebook::nimble {

class FlatMapLayoutPlanner : public LayoutPlanner {
 public:
  FlatMapLayoutPlanner(
      std::function<std::shared_ptr<const TypeBuilder>()> typeResolver,
      std::vector<std::tuple<size_t, std::vector<int64_t>>>
          flatMapFeatureOrder);

  virtual std::vector<Stream> getLayout(std::vector<Stream>&& streams) override;

 private:
  std::function<std::shared_ptr<const TypeBuilder>()> typeResolver_;
  std::vector<std::tuple<size_t, std::vector<int64_t>>> flatMapFeatureOrder_;
};
} // namespace facebook::nimble
