// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>

#include "dwio/alpha/velox/EncodingLayoutTree.h"

using namespace ::facebook;

namespace {
void verifyEncodingLayout(
    const std::optional<alpha::EncodingLayout>& expected,
    const std::optional<alpha::EncodingLayout>& actual) {
  ASSERT_EQ(expected.has_value(), actual.has_value());
  if (!expected.has_value()) {
    return;
  }

  ASSERT_EQ(expected->encodingType(), actual->encodingType());
  ASSERT_EQ(expected->compressionType(), actual->compressionType());
  ASSERT_EQ(expected->childrenCount(), actual->childrenCount());

  for (auto i = 0; i < expected->childrenCount(); ++i) {
    verifyEncodingLayout(expected->child(i), actual->child(i));
  }
}

void verifyEncodingLayoutTree(
    const alpha::EncodingLayoutTree& expected,
    const alpha::EncodingLayoutTree& actual) {
  ASSERT_EQ(expected.schemaKind(), actual.schemaKind());
  ASSERT_EQ(expected.name(), actual.name());
  ASSERT_EQ(expected.childrenCount(), actual.childrenCount());

  verifyEncodingLayout(expected.encodingLayout(), actual.encodingLayout());

  for (auto i = 0; i < expected.childrenCount(); ++i) {
    verifyEncodingLayoutTree(expected.child(i), actual.child(i));
  }
}

void test(const alpha::EncodingLayoutTree& expected) {
  std::string output;
  output.resize(2048);
  auto size = expected.serialize(output);

  auto actual = alpha::EncodingLayoutTree::create({output.data(), size});

  verifyEncodingLayoutTree(expected, actual);
}

} // namespace

TEST(EncodingLayoutTreeTests, SingleNode) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      alpha::EncodingLayout{
          alpha::EncodingType::SparseBool,
          alpha::CompressionType::Zstrong,
          {
              alpha::EncodingLayout{
                  alpha::EncodingType::FixedBitWidth,
                  alpha::CompressionType::Uncompressed},
          }},
      "  abc  ",
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, WithChildren) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      alpha::EncodingLayout{
          alpha::EncodingType::SparseBool,
          alpha::CompressionType::Zstrong,
          {
              alpha::EncodingLayout{
                  alpha::EncodingType::FixedBitWidth,
                  alpha::CompressionType::Uncompressed},
          }},
      "  abc  ",
      {
          {
              alpha::Kind::Scalar,
              alpha::EncodingLayout{
                  alpha::EncodingType::Trivial,
                  alpha::CompressionType::Zstd,
                  {
                      alpha::EncodingLayout{
                          alpha::EncodingType::Constant,
                          alpha::CompressionType::Uncompressed},
                  }},
              "  abc1  ",
          },
          {
              alpha::Kind::Array,
              std::nullopt,
              "",
          },
      },
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, SingleNodeNoEncoding) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      std::nullopt,
      "  abc  ",
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, SingleNodeEmptyName) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      alpha::EncodingLayout{
          alpha::EncodingType::SparseBool,
          alpha::CompressionType::Zstrong,
          {
              alpha::EncodingLayout{
                  alpha::EncodingType::FixedBitWidth,
                  alpha::CompressionType::Uncompressed},
          }},
      "",
  };

  test(expected);
}
