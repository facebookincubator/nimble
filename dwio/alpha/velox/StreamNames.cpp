// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/StreamNames.h"

#include "dwio/alpha/common/Exceptions.h"

namespace facebook::alpha {

StreamNames::StreamNames(const std::shared_ptr<const Type>& root) {
  // How many offsets and how many unique names?
  size_t offsetCount = 0;
  size_t uniqueNames = 0;
  SchemaReader::traverseSchema(
      root,
      [&](uint32_t level,
          const Type& type,
          const SchemaReader::NodeInfo& info) {
        ++offsetCount;
        if (level > 0) {
          switch (info.parentType->kind()) {
            case Kind::FlatMap:
              if (info.parentType->asFlatMap()
                      .inMapAt(info.placeInSibling / 2)
                      .get() == &type) {
                // inMap
                return;
              }
              break;
            case Kind::Array:
              // elements
              return;
            case Kind::Map:
              // keys, values
              return;
            case Kind::ArrayWithOffsets:
              // offsets, elements
              return;
            default:
              break;
          }
        }
        ++uniqueNames;
      });

  // Populate names
  names_.reserve(uniqueNames);
  toName_.resize(offsetCount);

  std::unordered_map<offset_size, std::string> label;
  SchemaReader::traverseSchema(
      root,
      [&](uint32_t level,
          const Type& type,
          const SchemaReader::NodeInfo& info) {
        if (level == 0) {
          toName_[type.offset()] = names_.size();
          names_.push_back("/");
        } else {
          std::string_view prefix = level > 1
              ? std::string_view(names_[toName_[info.parentType->offset()]])
              : std::string_view();
          switch (info.parentType->kind()) {
            case Kind::FlatMap:
              if (info.parentType->asFlatMap()
                      .inMapAt(info.placeInSibling / 2)
                      .get() == &type) {
                // inMap
                return;
              } else {
                // Real feature name.
                toName_[type.offset()] = names_.size();
                names_.push_back(
                    std::string(prefix) + std::string("/") +
                    std::string(info.name));
                // Fill inMap too
                toName_[info.parentType->asFlatMap()
                            .inMapAt(info.placeInSibling / 2)
                            ->offset()] = toName_[type.offset()];
                return;
              }
              break;
            case Kind::Array: // elements
                              // passthrough
            case Kind::Map: // keys, values
                            // passthrough
            case Kind::ArrayWithOffsets: // offsets, elements
              // Use parent's name
              toName_[type.offset()] = toName_[info.parentType->offset()];
              return;
            default:
              break;
          }
          toName_[type.offset()] = names_.size();
          names_.push_back(
              std::string(prefix) + std::string("/") + std::string(info.name));
        }
      });
}

std::string_view StreamNames::streamName(offset_size offset) const {
  ALPHA_ASSERT(offset < toName_.size(), "Stream offset out of range");
  return names_[toName_[offset]];
}

} // namespace facebook::alpha
