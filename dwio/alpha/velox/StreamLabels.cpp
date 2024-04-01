// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/StreamLabels.h"

#include "dwio/alpha/common/Exceptions.h"

namespace facebook::alpha {

namespace {
void addLabels(
    const std::shared_ptr<const Type>& node,
    std::vector<std::string>& labels,
    std::vector<size_t>& offsetToLabel,
    size_t labelIndex,
    const std::string& name) {
  switch (node->kind()) {
    case Kind::Scalar: {
      const auto& scalar = node->asScalar();
      const auto offset = scalar.scalarDescriptor().offset();
      ALPHA_DASSERT(labelIndex < labels.size(), "Unexpected label index.");
      ALPHA_DASSERT(offsetToLabel.size() > offset, "Unexpected offset.");
      labels.push_back(labels[labelIndex] + name);
      offsetToLabel[offset] = labels.size() - 1;
      break;
    }
    case Kind::Array: {
      const auto& array = node->asArray();
      const auto offset = array.lengthsDescriptor().offset();
      ALPHA_DASSERT(labelIndex < labels.size(), "Unexpected label index.");
      ALPHA_DASSERT(offsetToLabel.size() > offset, "Unexpected offset.");
      labels.push_back(labels[labelIndex] + name);
      labelIndex = labels.size() - 1;
      offsetToLabel[offset] = labelIndex;
      addLabels(array.elements(), labels, offsetToLabel, labelIndex, "");
      break;
    }
    case Kind::Map: {
      const auto& map = node->asMap();
      const auto offset = map.lengthsDescriptor().offset();
      ALPHA_DASSERT(labelIndex < labels.size(), "Unexpected label index.");
      ALPHA_DASSERT(offsetToLabel.size() > offset, "Unexpected offset.");
      labels.push_back(labels[labelIndex] + name);
      labelIndex = labels.size() - 1;
      offsetToLabel[offset] = labelIndex;
      addLabels(map.keys(), labels, offsetToLabel, labelIndex, "");
      addLabels(map.values(), labels, offsetToLabel, labelIndex, "");
      break;
    }
    case Kind::Row: {
      const auto& row = node->asRow();
      const auto offset = row.nullsDescriptor().offset();
      ALPHA_DASSERT(labelIndex < labels.size(), "Unexpected label index.");
      ALPHA_DASSERT(offsetToLabel.size() > offset, "Unexpected offset.");
      labels.push_back(labels[labelIndex] + "/");
      labelIndex = labels.size() - 1;
      offsetToLabel[offset] = labelIndex;
      for (auto i = 0; i < row.childrenCount(); ++i) {
        addLabels(
            row.childAt(i),
            labels,
            offsetToLabel,
            labelIndex,
            folly::to<std::string>(i));
      }
      break;
    }
    case Kind::FlatMap: {
      const auto& map = node->asFlatMap();
      const auto offset = map.nullsDescriptor().offset();
      ALPHA_DASSERT(labelIndex < labels.size(), "Unexpected label index.");
      ALPHA_DASSERT(offsetToLabel.size() > offset, "Unexpected offset.");
      labels.push_back(labels[labelIndex] + name);
      labelIndex = labels.size() - 1;
      offsetToLabel[offset] = labelIndex;
      for (auto i = 0; i < map.childrenCount(); ++i) {
        const auto inMapOffset = map.inMapDescriptorAt(i).offset();
        ALPHA_DASSERT(offsetToLabel.size() > inMapOffset, "Unexpected offset.");
        labels.push_back(labels[labelIndex] + "/" + map.nameAt(i));
        offsetToLabel[inMapOffset] = labels.size() - 1;
      }
      for (auto i = 0; i < map.childrenCount(); ++i) {
        addLabels(
            map.childAt(i),
            labels,
            offsetToLabel,
            offsetToLabel[map.inMapDescriptorAt(i).offset()],
            "");
      }
      break;
    }
    case Kind::ArrayWithOffsets: {
      const auto& array = node->asArrayWithOffsets();
      const auto offsetsOffset = array.offsetsDescriptor().offset();
      const auto lengthsOffset = array.lengthsDescriptor().offset();
      ALPHA_DASSERT(labelIndex < labels.size(), "Unexpected label index.");
      ALPHA_DASSERT(offsetToLabel.size() > offsetsOffset, "Unexpected offset.");
      ALPHA_DASSERT(offsetToLabel.size() > lengthsOffset, "Unexpected offset.");
      labels.push_back(labels[labelIndex] + name);
      labelIndex = labels.size() - 1;
      offsetToLabel[offsetsOffset] = labelIndex;
      offsetToLabel[lengthsOffset] = labelIndex;
      addLabels(array.elements(), labels, offsetToLabel, labelIndex, "");
      break;
    }
  }
}
} // namespace

StreamLabels::StreamLabels(const std::shared_ptr<const Type>& root) {
  size_t maxOffset = 0;
  size_t uniqueLabels = 1;
  SchemaReader::traverseSchema(
      root,
      [&](uint32_t level,
          const Type& type,
          const SchemaReader::NodeInfo& info) {
        switch (type.kind()) {
          case Kind::Scalar: {
            maxOffset = std::max<size_t>(
                maxOffset, type.asScalar().scalarDescriptor().offset());
            break;
          }
          case Kind::Array: {
            maxOffset = std::max<size_t>(
                maxOffset, type.asArray().lengthsDescriptor().offset());
            break;
          }
          case Kind::Map: {
            maxOffset = std::max<size_t>(
                maxOffset, type.asMap().lengthsDescriptor().offset());
            break;
          }
          case Kind::Row: {
            maxOffset = std::max<size_t>(
                maxOffset, type.asRow().nullsDescriptor().offset());
            uniqueLabels += type.asRow().childrenCount();
            break;
          }
          case Kind::FlatMap: {
            const auto& map = type.asFlatMap();
            maxOffset =
                std::max<size_t>(maxOffset, map.nullsDescriptor().offset());
            for (auto i = 0; i < map.childrenCount(); ++i) {
              maxOffset = std::max<size_t>(
                  maxOffset, map.inMapDescriptorAt(i).offset());
            }
            uniqueLabels += map.childrenCount();
            break;
          }
          case Kind::ArrayWithOffsets: {
            const auto& array = type.asArrayWithOffsets();
            maxOffset =
                std::max<size_t>(maxOffset, array.offsetsDescriptor().offset());
            maxOffset =
                std::max<size_t>(maxOffset, array.lengthsDescriptor().offset());
            break;
          }
        }
      });

  // Populate labels
  labels_.reserve(uniqueLabels + 1);
  offsetToLabel_.resize(maxOffset + 1);

  labels_.push_back("");
  addLabels(root, labels_, offsetToLabel_, 0, "");
}

std::string_view StreamLabels::streamLabel(offset_size offset) const {
  ALPHA_ASSERT(offset < offsetToLabel_.size(), "Stream offset out of range");
  return labels_[offsetToLabel_[offset]];
}

} // namespace facebook::alpha
