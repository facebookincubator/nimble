// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/StreamLabels.h"

#include "dwio/alpha/common/Exceptions.h"

namespace facebook::alpha {

StreamLabels::StreamLabels(const std::shared_ptr<const Type>& root) {
  // How many offsets and how many unique names?
  size_t offsetCount = 0;
  size_t uniqueLabels = 0;
  SchemaReader::traverseSchema(
      root,
      [&](uint32_t level,
          const Type& type,
          const SchemaReader::NodeInfo& info) {
        ++offsetCount;
        if (level > 0) {
          switch (info.parentType->kind()) {
            case Kind::FlatMap:
              // If this isn't the inMap stream of a parent flat map. See:
              // https://fburl.com/code/mn3bbugg
              if (info.parentType->asFlatMap()
                      .inMapAt(info.placeInSibling / 2)
                      .get() != &type) {
                // Feature name
                ++uniqueLabels;
              }
              break;
            case Kind::Row:
              // Ordinal name
              ++uniqueLabels;
              break;
            default:
              break;
          }
        }
      });

  // Populate labels
  labels_.reserve(uniqueLabels);
  offsetToLabel_.resize(offsetCount);

  SchemaReader::traverseSchema(
      root,
      [&](uint32_t level,
          const Type& type,
          const SchemaReader::NodeInfo& info) {
        if (level == 0) {
          offsetToLabel_[type.offset()] = labels_.size();
          labels_.emplace_back("/");
        } else {
          std::string_view prefix = level > 1
              ? std::string_view(
                    labels_[offsetToLabel_[info.parentType->offset()]])
              : std::string_view();
          switch (info.parentType->kind()) {
            case Kind::FlatMap:
              // If this isn't the inMap stream of a parent flat map. See:
              // https://fburl.com/code/mn3bbugg
              if (info.parentType->asFlatMap()
                      .inMapAt(info.placeInSibling / 2)
                      .get() != &type) {
                // Real feature name.
                offsetToLabel_[type.offset()] = labels_.size();
                labels_.push_back(
                    std::string(prefix) + std::string("/") +
                    std::string(info.name));

                // Fill inMap too
                offsetToLabel_[info.parentType->asFlatMap()
                                   .inMapAt(info.placeInSibling / 2)
                                   ->offset()] = offsetToLabel_[type.offset()];
              }
              // If this is the inMap we just ignore for now.
              break;
            case Kind::Row:
              // Use ordinal (place in sibling)
              offsetToLabel_[type.offset()] = labels_.size();
              labels_.push_back(
                  std::string(prefix) + std::string("/") +
                  std::to_string(info.placeInSibling));
              break;
            default:
              // Use parent's name
              offsetToLabel_[type.offset()] =
                  offsetToLabel_[info.parentType->offset()];
              break;
          }
        }
      });
}

std::string_view StreamLabels::streamLabel(offset_size offset) const {
  ALPHA_ASSERT(offset < offsetToLabel_.size(), "Stream offset out of range");
  return labels_[offsetToLabel_[offset]];
}

} // namespace facebook::alpha
