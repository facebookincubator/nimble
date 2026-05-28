/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "dwio/nimble/encodings/common/EncodingLayout.h"
#include <gtest/gtest.h>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
#include "dwio/nimble/encodings/SubIntSplitConfig.h"
#endif
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/common/EncodingFactory.h"
#include "dwio/nimble/tools/EncodingUtilities.h"
#include "velox/common/memory/Memory.h"

#include <unordered_map>

using namespace facebook;

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
          nimble::CompressionOptions{
              .compressionAcceptRatio = 100, .internalMinCompressionSize = 0},
          encodingSelectionPolicyFactory),
      data,
      buffer);

  auto actual = nimble::EncodingLayoutCapture::capture(encoding);
  verifyEncodingLayout(expected, actual);
}

#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
template <typename T>
class ForceSubIntSplitPolicy final : public nimble::EncodingSelectionPolicy<T> {
  using physicalType = typename nimble::TypeTraits<T>::physicalType;

 public:
  nimble::EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const nimble::Statistics<physicalType>& /* statistics */) override {
    return {.encodingType = nimble::EncodingType::SubIntSplit};
  }

  nimble::EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const nimble::Statistics<physicalType>& /* statistics */) override {
    return {.encodingType = nimble::EncodingType::Nullable};
  }

  std::unique_ptr<nimble::EncodingSelectionPolicyBase> createImpl(
      nimble::EncodingType /* encodingType */,
      nimble::NestedEncodingIdentifier /* identifier */,
      nimble::DataType type) override {
    nimble::ManualEncodingSelectionPolicyFactory factory{
        nimble::ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
        std::nullopt};
    return factory.createPolicy(type);
  }
};
#endif

} // namespace

TEST(EncodingLayoutTests, Trivial) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        {},
        nimble::CompressionType::Uncompressed};

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }

  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        {},
        nimble::CompressionType::MetaInternal};

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }
}

TEST(EncodingLayoutTests, TrivialString) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        {},
        nimble::CompressionType::Uncompressed,
        {
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                {},
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
        {},
        nimble::CompressionType::Uncompressed,
    };

    testSerialization(expected);
    testCapture<uint32_t>(expected, {1, 2, 3});
  }

  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::FixedBitWidth,
        {},
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
      {},
      nimble::CompressionType::Uncompressed,
  };

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 2, 3});
}

TEST(EncodingLayoutTests, Constant) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::Constant,
      {},
      nimble::CompressionType::Uncompressed,
  };

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1});
}

TEST(EncodingLayoutTests, SparseBool) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::SparseBool,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::MetaInternal},
      }};

  testSerialization(expected);
  testCapture<bool>(
      expected, std::array<bool, 5>{false, false, false, true, false});
}

TEST(EncodingLayoutTests, MainlyConst) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::MainlyConstant,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::MetaInternal},
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, Dictionary) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::Dictionary,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::FixedBitWidth,
              {},
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, Rle) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::RLE,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::FixedBitWidth,
              {},
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<uint32_t>(expected, {1, 1, 1, 1, 5, 1});
}

TEST(EncodingLayoutTests, RleBool) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::RLE,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(expected);
  testCapture<bool>(expected, std::array<bool, 4>{false, false, true, true});
}

TEST(EncodingLayoutTests, Nullable) {
  nimble::EncodingLayout expected{
      nimble::EncodingType::Nullable,
      {},
      nimble::CompressionType::Uncompressed,
      {nimble::EncodingLayout{
           nimble::EncodingType::FixedBitWidth,
           {},
           nimble::CompressionType::Uncompressed},
       nimble::EncodingLayout{
           nimble::EncodingType::SparseBool,
           {},
           nimble::CompressionType::Uncompressed,
           {
               nimble::EncodingLayout{
                   nimble::EncodingType::Trivial,
                   {},
                   nimble::CompressionType::MetaInternal},
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

#ifdef NIMBLE_ENABLE_EXPERIMENTAL_ENCODINGS
// SubIntSplitEncoding layout tests -------------------------------------------

// Verify that an EncodingLayout tree with a SubIntSplit root and two named
// child sections can be serialized and deserialized correctly.
TEST(EncodingLayoutTests, SubIntSplitSerialization) {
  nimble::EncodingLayout layout{
      nimble::EncodingType::SubIntSplit,
      {},
      nimble::CompressionType::Uncompressed,
      {
          // section 0: lower bits — typically FixedBitWidth or Trivial
          nimble::EncodingLayout{
              nimble::EncodingType::FixedBitWidth,
              {},
              nimble::CompressionType::Uncompressed},
          // section 1: upper bits — often Constant for structured IDs
          nimble::EncodingLayout{
              nimble::EncodingType::Constant,
              {},
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(layout);

  // Three sections
  nimble::EncodingLayout layout3{
      nimble::EncodingType::SubIntSplit,
      {},
      nimble::CompressionType::Uncompressed,
      {
          nimble::EncodingLayout{
              nimble::EncodingType::Trivial,
              {},
              nimble::CompressionType::Uncompressed},
          nimble::EncodingLayout{
              nimble::EncodingType::Dictionary,
              {},
              nimble::CompressionType::Uncompressed,
              {
                  nimble::EncodingLayout{
                      nimble::EncodingType::Trivial,
                      {},
                      nimble::CompressionType::Uncompressed},
                  nimble::EncodingLayout{
                      nimble::EncodingType::FixedBitWidth,
                      {},
                      nimble::CompressionType::Uncompressed},
              }},
          nimble::EncodingLayout{
              nimble::EncodingType::Constant,
              {},
              nimble::CompressionType::Uncompressed},
      }};

  testSerialization(layout3);
}

// Verify that EncodingLayoutCapture correctly reads the SubIntSplit binary
// format and that the captured tree round-trips through serialize/deserialize.
TEST(EncodingLayoutTests, SubIntSplitCapture) {
  nimble::EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{}](
          nimble::DataType dataType)
      -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  auto defaultPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*defaultPool};

  // Structured int64 values: upper 40 bits are a fixed datacenter/timestamp
  // prefix; lower 24 bits are a monotone counter. This should trigger
  // SubIntSplit selection (range << typeWidth) and produce a multi-section
  // encoding where the upper section is Constant or FixedBitWidth with small
  // range and the lower section is FixedBitWidth or Trivial.
  std::vector<int64_t> data;
  data.reserve(300);
  for (int64_t i = 0; i < 300; ++i) {
    data.push_back(static_cast<int64_t>(0x1234567890000000LL) | i);
  }

  auto encoding = nimble::EncodingFactory::encode<int64_t>(
      std::make_unique<ForceSubIntSplitPolicy<int64_t>>(),
      data,
      buffer);

  // Capture must succeed and not throw.
  auto captured = nimble::EncodingLayoutCapture::capture(encoding);
  ASSERT_EQ(captured.encodingType(), nimble::EncodingType::SubIntSplit);
  ASSERT_GT(captured.childrenCount(), 0u);
    EXPECT_EQ(
      captured.config().get(std::string(nimble::detail::subintsplit::kSplitModeConfigKey)),
      nimble::detail::subintsplit::kSplitModePreserve);
    ASSERT_TRUE(captured.config().get(
      std::string(nimble::detail::subintsplit::kSplitBoundariesConfigKey)).has_value());

  // The encoded stream must round-trip through the encoding factory.
  auto decoded = nimble::EncodingFactory().create(
      *defaultPool,
      encoding,
      [](uint32_t) { return nullptr; });
  nimble::Vector<int64_t> decodedValues{defaultPool.get()};
  decodedValues.resize(data.size());
  decoded->materialize(data.size(), decodedValues.data());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(data[i], decodedValues[i]) << i;
  }

  std::vector<nimble::EncodingType> traversedTypes;
  nimble::tools::traverseEncodings(
      encoding,
      [&](nimble::EncodingType encodingType,
          nimble::DataType dataType,
          uint32_t level,
          uint32_t /* index */,
          std::string nestedEncodingName,
          std::unordered_map<
              nimble::tools::EncodingPropertyType,
              nimble::tools::EncodingProperty> /* properties */) {
        if (level == 0) {
          EXPECT_EQ(encodingType, nimble::EncodingType::SubIntSplit);
          EXPECT_EQ(dataType, nimble::DataType::Int64);
          EXPECT_TRUE(nestedEncodingName.empty());
        }
        traversedTypes.push_back(encodingType);
        return true;
      });
  ASSERT_GE(traversedTypes.size(), 2u);

  // The captured layout must round-trip through serialize → deserialize.
  std::string output(4096, '\0');
  const auto serializedSize = captured.serialize(output);
  ASSERT_GT(serializedSize, 0);
  auto [deserialized, bytesRead] = nimble::EncodingLayout::create(
      {output.data(), static_cast<size_t>(serializedSize)});
  verifyEncodingLayout(captured, deserialized);
  auto preserveMode = deserialized.config().get(
      std::string(nimble::detail::subintsplit::kSplitModeConfigKey));
  ASSERT_TRUE(preserveMode.has_value());
  EXPECT_EQ(*preserveMode, nimble::detail::subintsplit::kSplitModePreserve);

  auto deserializedBoundaries = deserialized.config().get(
      std::string(nimble::detail::subintsplit::kSplitBoundariesConfigKey));
  auto capturedBoundaries = captured.config().get(
      std::string(nimble::detail::subintsplit::kSplitBoundariesConfigKey));
  ASSERT_TRUE(deserializedBoundaries.has_value());
  ASSERT_TRUE(capturedBoundaries.has_value());
  EXPECT_EQ(*deserializedBoundaries, *capturedBoundaries);

  // Each child must be non-nullopt (all sections were encoded).
  for (nimble::NestedEncodingIdentifier id = 0;
       id < captured.childrenCount();
       ++id) {
    EXPECT_TRUE(captured.child(id).has_value());
  }
}

TEST(EncodingLayoutTests, SubIntSplitPreserveBoundariesReplay) {
  nimble::EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = nimble::ManualEncodingSelectionPolicyFactory{}](
        nimble::DataType dataType)
      -> std::unique_ptr<nimble::EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  auto defaultPool = velox::memory::deprecatedAddDefaultLeafMemoryPool();
  nimble::Buffer buffer{*defaultPool};

  std::vector<int64_t> data;
  data.reserve(300);
  for (int64_t i = 0; i < 300; ++i) {
    data.push_back(static_cast<int64_t>(0x1234567890000000LL) | i);
  }

  const std::string preserveBoundaries = "0-15;16-47;48-63";
  nimble::EncodingLayout preserveLayout{
      nimble::EncodingType::SubIntSplit,
      nimble::EncodingLayout::Config{
          {{std::string(nimble::detail::subintsplit::kSplitModeConfigKey),
            std::string(nimble::detail::subintsplit::kSplitModePreserve)},
           {std::string(nimble::detail::subintsplit::kSplitBoundariesConfigKey),
            preserveBoundaries}}},
      nimble::CompressionType::Uncompressed,
      {std::nullopt, std::nullopt, std::nullopt}};

  auto encoding = nimble::EncodingFactory::encode<int64_t>(
      std::make_unique<nimble::ReplayedEncodingSelectionPolicy<int64_t>>(
          preserveLayout,
          std::nullopt,
          encodingSelectionPolicyFactory),
      data,
      buffer);

  auto captured = nimble::EncodingLayoutCapture::capture(encoding);
  ASSERT_EQ(captured.encodingType(), nimble::EncodingType::SubIntSplit);
  auto replayedMode = captured.config().get(
      std::string(nimble::detail::subintsplit::kSplitModeConfigKey));
  auto replayedBoundaries = captured.config().get(
      std::string(nimble::detail::subintsplit::kSplitBoundariesConfigKey));
  ASSERT_TRUE(replayedMode.has_value());
  ASSERT_TRUE(replayedBoundaries.has_value());
  EXPECT_EQ(*replayedMode, nimble::detail::subintsplit::kSplitModePreserve);
  EXPECT_EQ(*replayedBoundaries,
      preserveBoundaries);
  }
  EXPECT_EQ(*replayedBoundaries, preserveBoundaries);
}
#endif

TEST(EncodingLayoutTests, SizeTooSmall) {
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        {},
        nimble::CompressionType::Uncompressed,
    };

    std::string output;
    // Encoding needs minimum of 5 bytes. 4 is not enough.
    output.resize(4);
    EXPECT_THROW(
        expected.serialize(output), facebook::nimble::NimbleInternalError);
  }
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::Trivial,
        {},
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
        {},
        nimble::CompressionType::Uncompressed,
        {
            std::nullopt,
            std::nullopt,
        }};

    std::string output;
    // 5 bytes for the top level encoding, plus 2 "exists" bytes.
    // Total of 7 bytes. 6 bytes is not enough.
    output.resize(6);
    EXPECT_THROW(
        expected.serialize(output), facebook::nimble::NimbleInternalError);
  }
  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::MainlyConstant,
        {},
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
        {},
        nimble::CompressionType::Uncompressed,
        {
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                {},
                nimble::CompressionType::MetaInternal},
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                {},
                nimble::CompressionType::Uncompressed},
        }};

    std::string output;
    // Each sub-encoding is 5 bytes (total of 10), plus 5 for the top level one.
    // Plus 2 "exists" bytes. Total of 17 bytes. 16 bytes is not enough.
    output.resize(16);
    EXPECT_THROW(
        expected.serialize(output), facebook::nimble::NimbleInternalError);
  }

  {
    nimble::EncodingLayout expected{
        nimble::EncodingType::MainlyConstant,
        {},
        nimble::CompressionType::Uncompressed,
        {
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                {},
                nimble::CompressionType::MetaInternal},
            nimble::EncodingLayout{
                nimble::EncodingType::Trivial,
                {},
                nimble::CompressionType::Uncompressed},
        }};

    std::string output;
    // Each sub-encoding is 5 bytes (total of 10), plus 5 for the top level one.
    // Plus 2 "exists" bytes. Total of 17 bytes. 17 bytes is enough.
    output.resize(17);
    EXPECT_EQ(17, expected.serialize(output));
  }
}
