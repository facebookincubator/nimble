// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Exceptions.h"

namespace facebook::alpha {

namespace {

constexpr uint32_t kMinBufferSize = 9;

std::pair<EncodingLayoutTree, uint32_t> createInternal(std::string_view tree) {
  // Layout:
  // 1 byte: Schema Kind
  // 2 byte: Name length
  // X bytes: Name bytes
  // 2 byte: Encoding layout length
  // Y bytes: Encoding layout bytes
  // 4 byte: Children count
  // Z bytes: Children

  ALPHA_CHECK(
      tree.size() >= kMinBufferSize,
      "Invalid captured encoding tree. Buffer too small.");

  auto pos = tree.data();
  auto schemaKind = encoding::read<uint8_t, Kind>(pos);
  auto nameLength = encoding::read<uint16_t>(pos);

  ALPHA_CHECK(
      tree.size() >= nameLength + kMinBufferSize,
      "Invalid captured encoding tree. Buffer too small.");

  std::string_view name{pos, nameLength};
  pos += nameLength;

  auto encodingLength = encoding::read<uint16_t>(pos);

  ALPHA_CHECK(
      tree.size() >= nameLength + encodingLength + kMinBufferSize,
      "Invalid captured encoding tree. Buffer too small.");

  std::optional<EncodingLayout> encodingLayout;
  if (encodingLength > 0) {
    auto layout = EncodingLayout::create({pos, encodingLength});
    encodingLayout = std::move(layout.first);
    pos += layout.second;

    ALPHA_CHECK(
        layout.second == encodingLength,
        "Invalid captured encoding tree. Encoding size mismatch.");
  }

  auto childrenCount = encoding::read<uint32_t>(pos);
  uint32_t offset = pos - tree.data();
  std::vector<EncodingLayoutTree> children;
  children.reserve(childrenCount);
  for (auto i = 0; i < childrenCount; ++i) {
    auto encodingLayoutTree = createInternal(tree.substr(offset));
    offset += encodingLayoutTree.second;
    children.push_back(std::move(encodingLayoutTree.first));
  }

  return {
      {schemaKind,
       std::move(encodingLayout),
       std::string{name},
       std::move(children)},
      offset};
}

} // namespace

EncodingLayoutTree::EncodingLayoutTree(
    Kind schemaKind,
    std::optional<EncodingLayout> encodingLayout,
    std::string name,
    std::vector<EncodingLayoutTree> children)
    : schemaKind_{schemaKind},
      encodingLayout_{std::move(encodingLayout)},
      name_{std::move(name)},
      children_{std::move(children)} {}

uint32_t EncodingLayoutTree::serialize(std::span<char> output) const {
  ALPHA_CHECK(
      output.size() >= kMinBufferSize + name_.size(),
      "Captured encoding buffer too small.");

  auto pos = output.data();
  alpha::encoding::write(schemaKind_, pos);
  alpha::encoding::write<uint16_t>(name_.size(), pos);
  if (!name_.empty()) {
    alpha::encoding::writeBytes(name_, pos);
  }

  uint32_t encodingSize = 0;
  if (encodingLayout_.has_value()) {
    encodingSize = encodingLayout_->serialize(
        output.subspan(pos - output.data() + sizeof(uint16_t)));
  }
  alpha::encoding::write<uint16_t>(encodingSize, pos);
  pos += encodingSize;

  alpha::encoding::write<uint32_t>(children_.size(), pos);

  for (auto i = 0; i < children_.size(); ++i) {
    pos += children_[i].serialize(output.subspan(pos - output.data()));
  }

  return pos - output.data();
}

EncodingLayoutTree EncodingLayoutTree::create(std::string_view tree) {
  return std::move(createInternal(tree).first);
}

Kind EncodingLayoutTree::schemaKind() const {
  return schemaKind_;
}

const std::optional<EncodingLayout>& EncodingLayoutTree::encodingLayout()
    const {
  return encodingLayout_;
}

const std::string& EncodingLayoutTree::name() const {
  return name_;
}

uint32_t EncodingLayoutTree::childrenCount() const {
  return children_.size();
}

const EncodingLayoutTree& EncodingLayoutTree::child(uint32_t index) const {
  ALPHA_DCHECK(
      index < childrenCount(),
      "Encoding layout tree child index is out of range.");

  return children_[index];
}

} // namespace facebook::alpha
