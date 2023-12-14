// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/FlatMapLayoutPlanner.h"
#include <cstdint>

namespace facebook::alpha {

namespace {

void appendAllNestedStreams(
    const Type& type,
    std::vector<offset_size>& childrenOffsets) {
  childrenOffsets.push_back(type.offset());
  switch (type.kind()) {
    case Kind::Scalar: {
      break;
    }
    case Kind::Row: {
      auto& row = type.asRow();
      for (auto i = 0; i < row.childrenCount(); ++i) {
        appendAllNestedStreams(*row.childAt(i), childrenOffsets);
      }
      break;
    }
    case Kind::Array: {
      appendAllNestedStreams(*type.asArray().elements(), childrenOffsets);
      break;
    }
    case Kind::ArrayWithOffsets: {
      auto& arrayWithOffsets = type.asArrayWithOffsets();
      appendAllNestedStreams(*arrayWithOffsets.offsets(), childrenOffsets);
      appendAllNestedStreams(*arrayWithOffsets.elements(), childrenOffsets);
      break;
    }
    case Kind::Map: {
      auto& map = type.asMap();
      appendAllNestedStreams(*map.keys(), childrenOffsets);
      appendAllNestedStreams(*map.values(), childrenOffsets);
      break;
    }
    case Kind::FlatMap: {
      auto& flatMap = type.asFlatMap();
      for (auto i = 0; i < flatMap.childrenCount(); ++i) {
        childrenOffsets.push_back(flatMap.inMapAt(i)->offset());
        appendAllNestedStreams(*flatMap.childAt(i), childrenOffsets);
      }
      break;
    }
  }
}

} // namespace

FlatMapLayoutPlanner::FlatMapLayoutPlanner(
    std::function<std::shared_ptr<const Type>()> typeResolver,
    std::vector<std::tuple<size_t, std::vector<int64_t>>> flatMapFeatureOrder)
    : typeResolver_{std::move(typeResolver)},
      flatMapFeatureOrder_{std::move(flatMapFeatureOrder)} {
  ALPHA_ASSERT(typeResolver_ != nullptr, "typeResolver is not supplied");
}

std::vector<Stream> FlatMapLayoutPlanner::getLayout(
    std::vector<Stream>&& streams) {
  auto type = typeResolver_();
  ALPHA_ASSERT(
      type->kind() == Kind::Row,
      "Flat map layout planner requires row as the schema root.");
  auto& root = type->asRow();

  // Layout logic:
  // 1. Root stream (Row nulls) is always first
  // 2. Later, all flat maps included in config are layed out.
  //    For each map, we layout all the features included in the config for
  //    that map, in the order they appeared in the config. For each feature, we
  //    first add it's in-map stream and then all the value streams for that
  //    feature (if the value is a complex type, we add all the nested streams
  //    for this complex type together).
  // 3. We then layout all the other "leftover" streams, in "schema order". This
  //    guarantees that all "related" streams are next to each other.
  //    Leftover streams include all streams belonging to other columns, and all
  //    flat map features not included in the config.

  // This vector is going to hold all the ordered flat-map streams contained in
  // the config
  std::vector<offset_size> orderedFlatMapOffsets;
  orderedFlatMapOffsets.reserve(flatMapFeatureOrder_.size() * 3);

  for (const auto& flatMapFeatures : flatMapFeatureOrder_) {
    ALPHA_CHECK(
        std::get<0>(flatMapFeatures) < root.childrenCount(),
        fmt::format(
            "Column ordinal {} for feature ordering is out of range. "
            "Top-level row has {} columns.",
            std::get<0>(flatMapFeatures),
            root.childrenCount()));
    auto& column = root.childAt(std::get<0>(flatMapFeatures));
    ALPHA_CHECK(
        column->kind() == Kind::FlatMap,
        fmt::format(
            "Column '{}' for feature ordering is not a flat map.",
            root.nameAt(std::get<0>(flatMapFeatures))));

    // For each flat map, first we push the flat map (nulls) stream.
    orderedFlatMapOffsets.push_back(column->offset());

    auto& flatMap = column->asFlatMap();

    // Build a lookup table from feature name to its schema offset.
    std::unordered_map<std::string, offset_size> flatMapNamedOrdinals;
    flatMapNamedOrdinals.reserve(flatMap.childrenCount());
    for (auto i = 0; i < flatMap.childrenCount(); ++i) {
      flatMapNamedOrdinals.insert({flatMap.nameAt(i), i});
    }

    // For every ordered feature, check if we have streams for it and it, along
    // with all its nested streams to the ordered stream list.
    for (const auto& feature : std::get<1>(flatMapFeatures)) {
      auto it = flatMapNamedOrdinals.find(folly::to<std::string>(feature));
      if (it == flatMapNamedOrdinals.end()) {
        continue;
      }

      auto ordinal = it->second;
      auto& flatMapInMap = flatMap.inMapAt(ordinal);
      orderedFlatMapOffsets.push_back(flatMapInMap->offset());
      appendAllNestedStreams(*flatMap.childAt(ordinal), orderedFlatMapOffsets);
    }
  }

  // This vector is going to hold all the streams, ordered  based on schema
  // order. This will include streams that already appear in
  // 'orderedFlatMapOffsets'. Later, while laying out the final stream oder,
  // we'll de-dup these streams.
  std::vector<offset_size> orderedAllOffsets;
  appendAllNestedStreams(root, orderedAllOffsets);

  // Build a lookup table from type builders to their streams
  std::unordered_map<uint32_t, Stream*> offsetsToStreams;
  offsetsToStreams.reserve(streams.size());
  std::transform(
      streams.begin(),
      streams.end(),
      std::inserter(offsetsToStreams, offsetsToStreams.begin()),
      [](auto& stream) { return std::make_pair(stream.offset, &stream); });

  std::vector<Stream> layout;
  layout.reserve(streams.size());

  auto tryAppendStream = [&offsetsToStreams, &layout](uint32_t offset) {
    auto it = offsetsToStreams.find(offset);
    if (it != offsetsToStreams.end()) {
      layout.emplace_back(std::move(*it->second));
      offsetsToStreams.erase(it);
    }
  };

  // At this point we have ordered all the type builders, so now we are going to
  // try and find matching streams for each type builder and append them to the
  // final ordered stream list.

  // First add the root row
  tryAppendStream(root.offset());

  // Then, add all ordered flat maps
  for (auto offset : orderedFlatMapOffsets) {
    tryAppendStream(offset);
  }

  // Then add all "leftover" streams, in schema order.
  // 'tryAppendStream' will de-dup streams that were already added in previous
  // steps.
  for (auto offset : orderedAllOffsets) {
    tryAppendStream(offset);
  }

  ALPHA_ASSERT(
      streams.size() == layout.size(),
      fmt::format(
          "Stream count mismatch. Input size: {}, output size: {}.",
          streams.size(),
          layout.size()));

  return layout;
}
} // namespace facebook::alpha
