// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/encodings/EncodingLayout.h"
#include "dwio/alpha/encodings/EncodingLayoutCapture.h"
#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"

using namespace ::facebook;

namespace {

void verifyEncodingLayout(
    const std::optional<const alpha::EncodingLayout>& expected,
    const std::optional<const alpha::EncodingLayout>& actual) {
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

void testSerialization(alpha::EncodingLayout expected) {
  std::string output;
  output.resize(1024);
  auto size = expected.serialize(output);
  auto actual =
      alpha::EncodingLayout::create({output.data(), static_cast<size_t>(size)});
  verifyEncodingLayout(expected, actual.first);
}

template <typename T, typename TCollection = std::vector<T>>
void testCapture(alpha::EncodingLayout expected, TCollection data) {
  alpha::EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = alpha::ManualEncodingSelectionPolicyFactory{}](
          alpha::DataType dataType)
      -> std::unique_ptr<alpha::EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  auto defaultPool = velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*defaultPool};
  auto encoding = alpha::EncodingFactory::encode<T>(
      std::make_unique<alpha::ReplayedEncodingSelectionPolicy<T>>(
          expected,
          alpha::CompressionOptions{.compressionAcceptRatio = 100},
          encodingSelectionPolicyFactory),
      data,
      buffer);

  auto actual = alpha::EncodingLayoutCapture::capture(encoding);
  verifyEncodingLayout(expected, actual);
}

} // namespace

TEST(EncodingLayoutTests, Trivial) {
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::Trivial, alpha::CompressionType::Uncompressed};

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }

  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::Trivial, alpha::CompressionType::Zstrong};

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }
}

TEST(EncodingLayoutTests, TrivialString) {
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::Trivial,
        alpha::CompressionType::Uncompressed,
        {
            alpha::EncodingLayout{
                alpha::EncodingType::Trivial,
                alpha::CompressionType::Uncompressed},
        }};

    testSerialization(expected);
    testCapture<std::string_view>(expected, {"a", "b", "c"});
  }
}

TEST(EncodingLayoutTests, FixedBitWidth) {
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::FixedBitWidth,
        alpha::CompressionType::Uncompressed,
    };

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }

  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::FixedBitWidth,
        alpha::CompressionType::Zstd,
    };

    testSerialization(expected);
    // NOTE: We need this artitifical long input data, because if Zstd
    // compressed buffer is bigger than the uncompressed buffer, it is not
    // picked up, which then leads to the captured encloding layout to be
    // uncompressed.
    testCapture<uint32_t>(
        expected, {0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF,
                   0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF});
  }
}

TEST(EncodingLayoutTests, Varint) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::Varint,
      alpha::CompressionType::Uncompressed,
  };

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 2, 3});
}

TEST(EncodingLayoutTests, Constant) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::Constant,
      alpha::CompressionType::Uncompressed,
  };

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1});
}

TEST(EncodingLayoutTests, SparseBool) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::SparseBool,
      alpha::CompressionType::Uncompressed,
      {
          alpha::EncodingLayout{
              alpha::EncodingType::Trivial, alpha::CompressionType::Zstrong},
      }};

  testSerialization(expected);
  testCapture<bool>(
      expected, std::array<bool, 5>{false, false, false, true, false});
}

TEST(EncodingLayoutTests, MainlyConst) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::MainlyConstant,
      alpha::CompressionType::Uncompressed,
      {
          alpha::EncodingLayout{
              alpha::EncodingType::Trivial, alpha::CompressionType::Zstrong},
          alpha::EncodingLayout{
              alpha::EncodingType::Trivial,
              alpha::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, Dictionary) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::Dictionary,
      alpha::CompressionType::Uncompressed,
      {
          alpha::EncodingLayout{
              alpha::EncodingType::Trivial,
              alpha::CompressionType::Uncompressed},
          alpha::EncodingLayout{
              alpha::EncodingType::FixedBitWidth,
              alpha::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, Rle) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::RLE,
      alpha::CompressionType::Uncompressed,
      {
          alpha::EncodingLayout{
              alpha::EncodingType::Trivial,
              alpha::CompressionType::Uncompressed},
          alpha::EncodingLayout{
              alpha::EncodingType::FixedBitWidth,
              alpha::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, RleBool) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::RLE,
      alpha::CompressionType::Uncompressed,
      {
          alpha::EncodingLayout{
              alpha::EncodingType::Trivial,
              alpha::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<bool>(expected, std::array<bool, 4>{false, false, true, true});
}

TEST(EncodingLayoutTests, Nullable) {
  alpha::EncodingLayout expected{
      alpha::EncodingType::Nullable,
      alpha::CompressionType::Uncompressed,
      {alpha::EncodingLayout{
           alpha::EncodingType::FixedBitWidth,
           alpha::CompressionType::Uncompressed},
       alpha::EncodingLayout{
           alpha::EncodingType::SparseBool,
           alpha::CompressionType::Uncompressed,
           {
               alpha::EncodingLayout{
                   alpha::EncodingType::Trivial,
                   alpha::CompressionType::Zstrong},
           }}}};

  testSerialization(expected);

  alpha::EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = alpha::ManualEncodingSelectionPolicyFactory{}](
          alpha::DataType dataType)
      -> std::unique_ptr<alpha::EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  auto defaultPool = velox::memory::addDefaultLeafMemoryPool();
  alpha::Buffer buffer{*defaultPool};
  auto encoding = alpha::EncodingFactory::encodeNullable<uint32_t>(
      std::make_unique<alpha::ReplayedEncodingSelectionPolicy<uint32_t>>(
          expected.child(alpha::EncodingIdentifiers::Nullable::Data).value(),
          alpha::CompressionOptions{},
          encodingSelectionPolicyFactory),
      std::vector<uint32_t>{1, 1, 1, 1, 5, 1},
      std::array<bool, 6>{false, false, true, false, false, false},
      buffer);

  std::string output;
  output.resize(1024);
  auto captured = alpha::EncodingLayoutCapture::capture(encoding);
  auto size = captured.serialize(output);

  auto actual =
      alpha::EncodingLayout::create({output.data(), static_cast<size_t>(size)});

  // For nullable, captured encoding layout strips out the nullable node and
  // just captures the data node.
  verifyEncodingLayout(
      expected.child(alpha::EncodingIdentifiers::Nullable::Data), actual.first);
}

TEST(EncodingLayoutTests, SizeTooSmall) {
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::Trivial,
        alpha::CompressionType::Uncompressed,
    };

    std::string output;
    // Encoding needs minimum of 5 bytes. 4 is not enough.
    output.resize(4);
    EXPECT_THROW(expected.serialize(output), facebook::alpha::AlphaUserError);
  }
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::Trivial,
        alpha::CompressionType::Uncompressed,
    };

    std::string output;
    // Encoding needs minimum of 5 bytes. Should not throw.
    output.resize(5);
    EXPECT_EQ(5, expected.serialize(output));
  }
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::MainlyConstant,
        alpha::CompressionType::Uncompressed,
        {
            std::nullopt,
            std::nullopt,
        }};

    std::string output;
    // 5 bytes for the top level encoding, plus 2 "exists" bytes.
    // Total of 7 bytes. 6 bytes is not enough.
    output.resize(6);
    EXPECT_THROW(expected.serialize(output), facebook::alpha::AlphaUserError);
  }
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::MainlyConstant,
        alpha::CompressionType::Uncompressed,
        {
            std::nullopt,
            std::nullopt,
        }};

    std::string output;
    // 5 bytes for the top level encoding, plus 2 "exists" bytes.
    // Total of 7 bytes. 7 bytes is enough.
    output.resize(7);
    EXPECT_EQ(7, expected.serialize(output));
  }
  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::MainlyConstant,
        alpha::CompressionType::Uncompressed,
        {
            alpha::EncodingLayout{
                alpha::EncodingType::Trivial, alpha::CompressionType::Zstrong},
            alpha::EncodingLayout{
                alpha::EncodingType::Trivial,
                alpha::CompressionType::Uncompressed},
        }};

    std::string output;
    // Each sub-encoding is 5 bytes (total of 10), plus 5 for the top level one.
    // Plus 2 "exists" bytes. Total of 17 bytes. 16 bytes is not enough.
    output.resize(16);
    EXPECT_THROW(expected.serialize(output), facebook::alpha::AlphaUserError);
  }

  {
    alpha::EncodingLayout expected{
        alpha::EncodingType::MainlyConstant,
        alpha::CompressionType::Uncompressed,
        {
            alpha::EncodingLayout{
                alpha::EncodingType::Trivial, alpha::CompressionType::Zstrong},
            alpha::EncodingLayout{
                alpha::EncodingType::Trivial,
                alpha::CompressionType::Uncompressed},
        }};

    std::string output;
    // Each sub-encoding is 5 bytes (total of 10), plus 5 for the top level one.
    // Plus 2 "exists" bytes. Total of 17 bytes. 17 bytes is enough.
    output.resize(17);
    EXPECT_EQ(17, expected.serialize(output));
  }
}
