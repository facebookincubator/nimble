// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>

#include "dwio/alpha/velox/EncodingLayoutTree.h"

using namespace ::facebook;

namespace {

std::optional<alpha::EncodingLayout> cloneAsOptional(
    const alpha::EncodingLayout* encodingLayout) {
  if (!encodingLayout) {
    return std::nullopt;
  }

  std::string output;
  output.resize(1024);
  auto size = encodingLayout->serialize(output);
  return {
      alpha::EncodingLayout::create({output.data(), static_cast<size_t>(size)})
          .first};
}

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

  for (uint8_t i = 0; i < std::numeric_limits<uint8_t>::max(); ++i) {
    verifyEncodingLayout(
        cloneAsOptional(expected.encodingLayout(i)),
        cloneAsOptional(actual.encodingLayout(i)));
  }

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
      {{
          alpha::EncodingLayoutTree::StreamIdentifiers::Row::NullsStream,
          alpha::EncodingLayout{
              alpha::EncodingType::SparseBool,
              alpha::CompressionType::Zstrong,
              {
                  alpha::EncodingLayout{
                      alpha::EncodingType::FixedBitWidth,
                      alpha::CompressionType::Uncompressed},
              }},
      }},
      "  abc  ",
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, SingleNodeMultipleStreams) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {
          {
              2,
              alpha::EncodingLayout{
                  alpha::EncodingType::SparseBool,
                  alpha::CompressionType::Zstrong,
                  {
                      alpha::EncodingLayout{
                          alpha::EncodingType::FixedBitWidth,
                          alpha::CompressionType::Uncompressed},
                  }},
          },
          {
              4,
              alpha::EncodingLayout{
                  alpha::EncodingType::Dictionary,
                  alpha::CompressionType::Zstd,
                  {
                      alpha::EncodingLayout{
                          alpha::EncodingType::Constant,
                          alpha::CompressionType::Uncompressed},
                  }},
          },
      },
      "  abcd  ",
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, WithChildren) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {
          {
              1,
              alpha::EncodingLayout{
                  alpha::EncodingType::SparseBool,
                  alpha::CompressionType::Zstrong,
                  {
                      alpha::EncodingLayout{
                          alpha::EncodingType::FixedBitWidth,
                          alpha::CompressionType::Uncompressed},
                  }},
          },
      },
      "  abc  ",
      {
          {
              alpha::Kind::Scalar,
              {
                  {
                      0,
                      alpha::EncodingLayout{
                          alpha::EncodingType::Trivial,
                          alpha::CompressionType::Zstd,
                          {
                              alpha::EncodingLayout{
                                  alpha::EncodingType::Constant,
                                  alpha::CompressionType::Uncompressed},
                          }},
                  },
              },
              "  abc1  ",
          },
          {
              alpha::Kind::Array,
              {},
              "",
          },
      },
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, SingleNodeNoEncoding) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {},
      "  abc  ",
  };

  test(expected);
}

TEST(EncodingLayoutTreeTests, SingleNodeEmptyName) {
  alpha::EncodingLayoutTree expected{
      alpha::Kind::Row,
      {
          {
              9,
              alpha::EncodingLayout{
                  alpha::EncodingType::SparseBool,
                  alpha::CompressionType::Zstrong,
                  {
                      alpha::EncodingLayout{
                          alpha::EncodingType::FixedBitWidth,
                          alpha::CompressionType::Uncompressed},
                  }},
          },
      },
      "",
  };

  test(expected);
}
