// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/tablet/Tablet.h"
#include "dwio/alpha/velox/SchemaReader.h"

namespace facebook::alpha {

class FlatMapLayoutPlanner : public LayoutPlanner {
 public:
  FlatMapLayoutPlanner(
      std::function<std::shared_ptr<const Type>()> typeResolver,
      std::vector<std::tuple<size_t, std::vector<int64_t>>>
          flatMapFeatureOrder);

  virtual std::vector<Stream> getLayout(std::vector<Stream>&& streams) override;

 private:
  std::function<std::shared_ptr<const Type>()> typeResolver_;
  std::vector<std::tuple<size_t, std::vector<int64_t>>> flatMapFeatureOrder_;
};
} // namespace facebook::alpha
