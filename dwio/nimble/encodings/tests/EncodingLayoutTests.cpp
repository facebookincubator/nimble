// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/EncodingLayoutCapture.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"

using namespace ::facebook;

namespace {

void verifyEncodingLayout(
    const std::optional<const nimble::EncodingLayout>& expected,
    const std::optional<const nimble::EncodingLayout>& actual) {
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

void testSerialization(nimble::EncodingLayout expected) {
  std::string output;
  output.resize(1024);
  auto size = expected.serialize(output);
  auto actual = nimble::EncodingLayout::create(
      {output.data(), static_cast<size_t>(size)});
  verifyEncodingLayout(expected, actual.first);
}

template <typename T, typename TCollection = std::vector<T>>
void testCapture(nimble::EncodingLayout expected, TCollection data) {
  nimble::EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{}](
          nimble::DataType dataType)
      -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  auto defaultPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*defaultPool};
  auto encoding = nimble::EncodingFactory::encode<T>(
      std::make_unique<nimble::ReplayedEncodingSelectionPolicy<T>>(
          expected,
          nimble::CompressionOptions{.compressionAcceptRatio = 100},
          encodingSelectionPolicyFactory),
      data,
      buffer);

  auto actual = nimble::EncodingLayoutCapture::capture(encoding);
  verifyEncodingLayout(expected, actual);
}

} // namespace

TEST(EncodingLayoutTests, Trivial) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial, nimble::CompressionType::Uncompressed};

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }

  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial, nimble::CompressionType::Zstrong};

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }
}

TEST(EncodingLayoutTests, TrivialString) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        nimble::CompressionType::Uncompressed,
        {
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                nimble::CompressionType::Uncompressed},
        }};

    testSerialization(expected);
    testCapture<std::string_view>(expected, {"a", "b", "c"});
  }
}

TEST(EncodingLayoutTests, FixedBitWidth) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::FixedBitWidth,
        nimble::CompressionType::Uncompressed,
    };

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }

  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::FixedBitWidth,
        nimble::CompressionType::Zstd,
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
  nimble::EncodingLayout expected{
      nimble::EncodingType::Varint,
      nimble::CompressionType::Uncompressed,
  };

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 2, 3});
}

TEST(EncodingLayoutTests, Constant) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::Constant,
      nimble::CompressionType::Uncompressed,
  };

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1});
}

TEST(EncodingLayoutTests, SparseBool) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::SparseBool,
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial, nimble::CompressionType::Zstrong},
      }};

  testSerialization(expected);
  testCapture<bool>(
      expected, std::array<bool, 5>{false, false, false, true, false});
}

TEST(EncodingLayoutTests, MainlyConst) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::MainlyConstant,
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial, nimble::CompressionType::Zstrong},
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, Dictionary) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::Dictionary,
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::FixedBitWidth,
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, Rle) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::RLE,
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::FixedBitWidth,
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, RleBool) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::RLE,
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<bool>(expected, std::array<bool, 4>{false, false, true, true});
}

TEST(EncodingLayoutTests, Nullable) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::Nullable,
      nimble::CompressionType::Uncompressed,
      {nimble::EncodingLayout{
           nimble::EncodingType::FixedBitWidth,
           nimble::CompressionType::Uncompressed},
       nimble::EncodingLayout{
           nimble::EncodingType::SparseBool,
           nimble::CompressionType::Uncompressed,
           {
               nimble::EncodingLayout{
                   nimble::EncodingType::Trivial,
                   nimble::CompressionType::Zstrong},
           }}}};

  testSerialization(expected);

  nimble::EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{}](
          nimble::DataType dataType)
      -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  auto defaultPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*defaultPool};
  auto encoding = nimble::EncodingFactory::encodeNullable<uint32_t>(
      std::make_unique<nimble::ReplayedEncodingSelectionPolicy<uint32_t>>(
          expected.child(nimble::EncodingIdentifiers::Nullable::Data).value(),
          nimble::CompressionOptions{},
          encodingSelectionPolicyFactory),
      std::vector<uint32_t>{1, 1, 1, 1, 5, 1},
      std::array<bool, 6>{false, false, true, false, false, false},
      buffer);

  std::string output;
  output.resize(1024);
  auto captured = nimble::EncodingLayoutCapture::capture(encoding);
  auto size = captured.serialize(output);

  auto actual = nimble::EncodingLayout::create(
      {output.data(), static_cast<size_t>(size)});

  // For nullable, captured encoding layout strips out the nullable node and
  // just captures the data node.
  verifyEncodingLayout(
      expected.child(nimble::EncodingIdentifiers::Nullable::Data),
      actual.first);
}

TEST(EncodingLayoutTests, SizeTooSmall) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        nimble::CompressionType::Uncompressed,
    };

    std::string output;
    // Encoding needs minimum of 5 bytes. 4 is not enough.
    output.resize(4);
    EXPECT_THROW(expected.serialize(output), facebook::nimble::NimbleUserError);
  }
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        nimble::CompressionType::Uncompressed,
    };

    std::string output;
    // Encoding needs minimum of 5 bytes. Should not throw.
    output.resize(5);
    EXPECT_EQ(5, expected.serialize(output));
  }
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::MainlyConstant,
        nimble::CompressionType::Uncompressed,
        {
            std::nullopt,
            std::nullopt,
        }};

    std::string output;
    // 5 bytes for the top level encoding, plus 2 "exists" bytes.
    // Total of 7 bytes. 6 bytes is not enough.
    output.resize(6);
    EXPECT_THROW(expected.serialize(output), facebook::nimble::NimbleUserError);
  }
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::MainlyConstant,
        nimble::CompressionType::Uncompressed,
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
    nimble::EncodingLayout expected{
        nimble::EncodingType::MainlyConstant,
        nimble::CompressionType::Uncompressed,
        {
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                nimble::CompressionType::Zstrong},
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                nimble::CompressionType::Uncompressed},
        }};

    std::string output;
    // Each sub-encoding is 5 bytes (total of 10), plus 5 for the top level one.
    // Plus 2 "exists" bytes. Total of 17 bytes. 16 bytes is not enough.
    output.resize(16);
    EXPECT_THROW(expected.serialize(output), facebook::nimble::NimbleUserError);
  }

  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::MainlyConstant,
        nimble::CompressionType::Uncompressed,
        {
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                nimble::CompressionType::Zstrong},
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                nimble::CompressionType::Uncompressed},
        }};

    std::string output;
    // Each sub-encoding is 5 bytes (total of 10), plus 5 for the top level one.
    // Plus 2 "exists" bytes. Total of 17 bytes. 17 bytes is enough.
    output.resize(17);
    EXPECT_EQ(17, expected.serialize(output));
  }
}
