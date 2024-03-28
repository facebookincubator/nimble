// Copyright 2004-present Facebook. All Rights Reserved.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <vector>
#include "dwio/alpha/common/Bits.h"
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/FixedBitArray.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/common/Vector.h"
#include "dwio/alpha/common/tests/TestUtils.h"
#include "dwio/alpha/encodings/ConstantEncoding.h"
#include "dwio/alpha/encodings/DeltaEncoding.h"
#include "dwio/alpha/encodings/DictionaryEncoding.h"
#include "dwio/alpha/encodings/Encoding.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"
#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"
#include "dwio/alpha/encodings/FixedBitWidthEncoding.h"
#include "dwio/alpha/encodings/MainlyConstantEncoding.h"
#include "dwio/alpha/encodings/RleEncoding.h"
#include "dwio/alpha/encodings/SparseBoolEncoding.h"
#include "dwio/alpha/encodings/TrivialEncoding.h"
#include "dwio/alpha/encodings/VarintEncoding.h"
#include "folly/Random.h"
#include "folly/container/F14Set.h"
#include "velox/common/memory/Memory.h"

// Tests the Encoding API for all basic Encoding implementations and data types.
//
// Nullable data will be tested via a separate file, as will structured data,
// and any data-type-specific functions (e.g. for there are some Encoding calls
// only valid for bools, and those are testing in BoolEncodingTests.cpp).
//
// The tests are templated so as to cover all data types and encoding types with
// a single code path.

using namespace ::facebook;

namespace {

class TestCompressPolicy : public alpha::CompressionPolicy {
 public:
  explicit TestCompressPolicy(bool compress, bool useVariableBitWidthCompressor)
      : compress_{compress},
        useVariableBitWidthCompressor_{useVariableBitWidthCompressor} {}

  alpha::CompressionInformation compression() const override {
    if (!compress_) {
      return {.compressionType = alpha::CompressionType::Uncompressed};
    }

    alpha::CompressionInformation information{
        .compressionType = alpha::CompressionType::Zstrong};
    information.parameters.zstrong.compressionLevel = 9;
    information.parameters.zstrong.decompressionLevel = 2;
    information.parameters.zstrong.useVariableBitWidthCompressor =
        useVariableBitWidthCompressor_;
    return information;
  }

  virtual bool shouldAccept(
      alpha::CompressionType /* compressionType */,
      uint64_t /* uncompressedSize */,
      uint64_t /* compressedSize */) const override {
    return true;
  }

 private:
  bool compress_;
  bool useVariableBitWidthCompressor_;
};

template <typename T>
class TestTrivialEncodingSelectionPolicy
    : public alpha::EncodingSelectionPolicy<T> {
  using physicalType = typename alpha::TypeTraits<T>::physicalType;

 public:
  explicit TestTrivialEncodingSelectionPolicy(
      bool shouldCompress,
      bool useVariableBitWidthCompressor)
      : shouldCompress_{shouldCompress},
        useVariableBitWidthCompressor_{useVariableBitWidthCompressor} {}

  alpha::EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const alpha::Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = alpha::EncodingType::Trivial,
        .compressionPolicyFactory = [this]() {
          return std::make_unique<TestCompressPolicy>(
              shouldCompress_, useVariableBitWidthCompressor_);
        }};
  }

  alpha::EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const alpha::Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = alpha::EncodingType::Nullable,
    };
  }

  std::unique_ptr<alpha::EncodingSelectionPolicyBase> createImpl(
      alpha::EncodingType /* encodingType */,
      alpha::NestedEncodingIdentifier /* identifier */,
      alpha::DataType type) override {
    UNIQUE_PTR_FACTORY(
        type,
        TestTrivialEncodingSelectionPolicy,
        shouldCompress_,
        useVariableBitWidthCompressor_);
  }

 private:
  bool shouldCompress_;
  bool useVariableBitWidthCompressor_;
};
} // namespace
// C is the encoding type.
template <typename C>
class EncodingTests : public ::testing::Test {
 protected:
  using E = typename C::cppDataType;

  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<alpha::Buffer>(*this->pool_);
    util_ = std::make_unique<alpha::testing::Util>(*this->pool_);
  }

  template <typename Encoding>
  struct EncodingTypeTraits {};

  template <>
  struct EncodingTypeTraits<alpha::ConstantEncoding<E>> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::Constant;
  };

  template <>
  struct EncodingTypeTraits<alpha::DictionaryEncoding<E>> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::Dictionary;
  };

  template <>
  struct EncodingTypeTraits<alpha::FixedBitWidthEncoding<E>> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::FixedBitWidth;
  };

  template <>
  struct EncodingTypeTraits<alpha::MainlyConstantEncoding<E>> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::MainlyConstant;
  };

  template <>
  struct EncodingTypeTraits<alpha::RLEEncoding<E>> {
    static inline alpha::EncodingType encodingType = alpha::EncodingType::RLE;
  };

  template <>
  struct EncodingTypeTraits<alpha::SparseBoolEncoding> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::SparseBool;
  };

  template <>
  struct EncodingTypeTraits<alpha::TrivialEncoding<E>> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::Trivial;
  };

  template <>
  struct EncodingTypeTraits<alpha::VarintEncoding<E>> {
    static inline alpha::EncodingType encodingType =
        alpha::EncodingType::Varint;
  };

  std::unique_ptr<alpha::Encoding> createEncoding(
      const alpha::Vector<E>& values,
      bool compress,
      bool useVariableBitWidthCompressor) {
    using physicalType = typename alpha::TypeTraits<E>::physicalType;
    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    alpha::EncodingSelection<physicalType> selection{
        {.encodingType = EncodingTypeTraits<C>::encodingType,
         .compressionPolicyFactory =
             [compress, useVariableBitWidthCompressor]() {
               return std::make_unique<TestCompressPolicy>(
                   compress, useVariableBitWidthCompressor);
             }},
        alpha::Statistics<physicalType>::create(physicalValues),
        std::make_unique<TestTrivialEncodingSelectionPolicy<E>>(
            compress, useVariableBitWidthCompressor)};

    auto encoded = C::encode(selection, physicalValues, *buffer_);
    return std::make_unique<C>(*this->pool_, encoded);
  }

  // Each unit test runs on randomized data this many times before
  // we conclude the unit test passed.
  static constexpr int32_t kNumRandomRuns = 10;

  // We want the number of row tested to potentially be large compared to a
  // skip block. When we actually generate data we pick a random length between
  // 1 and this size.
  static constexpr int32_t kMaxRows = 2000;

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<alpha::Buffer> buffer_;
  std::unique_ptr<alpha::testing::Util> util_;
};

#define VARINT_TYPES(EncodingName)                                      \
  EncodingName<int32_t>, EncodingName<int64_t>, EncodingName<uint32_t>, \
      EncodingName<uint64_t>, EncodingName<float>, EncodingName<double>

#define NUMERIC_TYPES(EncodingName)                                        \
  VARINT_TYPES(EncodingName), EncodingName<int8_t>, EncodingName<uint8_t>, \
      EncodingName<int16_t>, EncodingName<uint16_t>

#define NON_BOOL_TYPES(EncodingName) \
  NUMERIC_TYPES(EncodingName), EncodingName<std::string_view>

#define ALL_TYPES(EncodingName) NON_BOOL_TYPES(EncodingName), EncodingName<bool>

using TestTypes = ::testing::Types<
    alpha::SparseBoolEncoding,
    VARINT_TYPES(alpha::VarintEncoding),
    NUMERIC_TYPES(alpha::FixedBitWidthEncoding),
    NON_BOOL_TYPES(alpha::DictionaryEncoding),
    NON_BOOL_TYPES(alpha::MainlyConstantEncoding),
    ALL_TYPES(alpha::TrivialEncoding),
    ALL_TYPES(alpha::RLEEncoding),
    ALL_TYPES(alpha::ConstantEncoding)>;

TYPED_TEST_CASE(EncodingTests, TestTypes);

TYPED_TEST(EncodingTests, Materialize) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  using E = typename TypeParam::cppDataType;

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<alpha::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto compress : {false, true}) {
        for (auto useVariableBitWidthCompressor : {false, true}) {
          const int rowCount = data.size();
          ASSERT_GT(rowCount, 0);
          std::unique_ptr<alpha::Encoding> encoding;
          try {
            encoding = this->createEncoding(
                data, compress, useVariableBitWidthCompressor);
          } catch (const alpha::AlphaUserError& e) {
            if (e.errorCode() == alpha::error_code::IncompatibleEncoding) {
              continue;
            }
            throw;
          }
          ASSERT_EQ(encoding->dataType(), alpha::TypeTraits<E>::dataType);
          alpha::Vector<E> buffer(this->pool_.get(), rowCount);

          encoding->materialize(rowCount, buffer.data());
          for (int i = 0; i < rowCount; ++i) {
            ASSERT_EQ(buffer[i], data[i]) << "i: " << i;
          }

          encoding->reset();
          const int firstBlock = rowCount / 2;
          encoding->materialize(firstBlock, buffer.data());
          for (int i = 0; i < firstBlock; ++i) {
            ASSERT_EQ(buffer[i], data[i]);
          }
          const int secondBlock = rowCount - firstBlock;
          encoding->materialize(secondBlock, buffer.data());
          for (int i = 0; i < secondBlock; ++i) {
            ASSERT_EQ(buffer[i], data[firstBlock + i]);
          }

          encoding->reset();
          for (int i = 0; i < rowCount; ++i) {
            encoding->materialize(1, buffer.data());
            ASSERT_EQ(buffer[0], data[i]);
          }

          encoding->reset();
          int start = 0;
          int len = 0;
          for (int i = 0; i < rowCount; ++i) {
            start += len;
            len += 1;
            if (start + len > rowCount) {
              break;
            }
            encoding->materialize(len, buffer.data());
            for (int j = 0; j < len; ++j) {
              ASSERT_EQ(data[start + j], buffer[j]);
            }
          }

          const uint32_t offset = folly::Random::rand32(rng) % data.size();
          const uint32_t length =
              1 + folly::Random::rand32(rng) % (data.size() - offset);
          encoding->reset();
          encoding->skip(offset);
          encoding->materialize(length, buffer.data());
          for (uint32_t i = 0; i < length; ++i) {
            ASSERT_EQ(buffer[i], data[offset + i]);
          }
        }
      }
    }
  }
}

template <typename T>
void checkScatteredOutput(
    bool hasNulls,
    size_t index,
    const bool* scatter,
    const T* actual,
    const T* expected,
    const char* nulls,
    uint32_t& expectedRow) {
  if (scatter[index]) {
    ASSERT_EQ(actual[index], expected[expectedRow]);
    ++expectedRow;
  }

  if (hasNulls) {
    ASSERT_EQ(scatter[index], alpha::bits::getBit(index, nulls));
  }
}

TYPED_TEST(EncodingTests, ScatteredMaterialize) {
  using E = typename TypeParam::cppDataType;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<alpha::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto compress : {false, true}) {
        for (auto useVariableBitWidthCompressor : {false, true}) {
          const int rowCount = data.size();
          ASSERT_GT(rowCount, 0);
          std::unique_ptr<alpha::Encoding> encoding;
          try {
            encoding = this->createEncoding(
                data, compress, useVariableBitWidthCompressor);
          } catch (const alpha::AlphaUserError& e) {
            if (e.errorCode() == alpha::error_code::IncompatibleEncoding) {
              continue;
            }
            throw;
          }
          ASSERT_EQ(encoding->dataType(), alpha::TypeTraits<E>::dataType);

          int setBits = 0;
          std::vector<int32_t> scatterSizes(rowCount + 1);
          scatterSizes[0] = 0;
          alpha::Vector<bool> scatter(this->pool_.get());
          while (setBits < rowCount) {
            scatter.push_back(folly::Random::oneIn(2, rng));
            if (scatter.back()) {
              scatterSizes[++setBits] = scatter.size();
            }
          }

          auto newRowCount = scatter.size();
          auto requiredBytes = alpha::bits::bytesRequired(newRowCount);
          // Note: Internally, some bit implementations use word boundaries to
          // efficiently iterate on bitmaps. If the buffer doesn't end on a word
          // boundary, this leads to ASAN buffer overflow (debug builds). So for
          // now, we are allocating extra 7 bytes to make sure the buffer ends
          // or exceeds a word boundary.
          alpha::Buffer scatterBuffer{*this->pool_, requiredBytes + 7};
          alpha::Buffer nullsBuffer{*this->pool_, requiredBytes + 7};
          auto scatterPtr = scatterBuffer.reserve(requiredBytes);
          auto nullsPtr = nullsBuffer.reserve(requiredBytes);
          memset(scatterPtr, 0, requiredBytes);
          alpha::bits::packBitmap(scatter, scatterPtr);

          alpha::Vector<E> buffer(this->pool_.get(), newRowCount);

          uint32_t expectedRow = 0;
          uint32_t actualRows = 0;
          {
            alpha::bits::Bitmap scatterBitmap(scatterPtr, newRowCount);
            actualRows = encoding->materializeNullable(
                rowCount,
                buffer.data(),
                [&]() { return nullsPtr; },
                &scatterBitmap);
          }
          ASSERT_EQ(actualRows, rowCount);
          auto hasNulls = actualRows != newRowCount;
          for (int i = 0; i < newRowCount; ++i) {
            checkScatteredOutput(
                hasNulls,
                i,
                scatter.data(),
                buffer.data(),
                data.data(),
                nullsPtr,
                expectedRow);
          }
          EXPECT_EQ(rowCount, expectedRow);

          encoding->reset();
          const int firstBlock = rowCount / 2;
          const int firstScatterSize = scatterSizes[firstBlock];
          expectedRow = 0;
          {
            alpha::bits::Bitmap scatterBitmap(scatterPtr, firstScatterSize);
            actualRows = encoding->materializeNullable(
                firstBlock,
                buffer.data(),
                [&]() { return nullsPtr; },
                &scatterBitmap);
          }
          ASSERT_EQ(actualRows, firstBlock);
          hasNulls = actualRows != firstScatterSize;
          for (int i = 0; i < firstScatterSize; ++i) {
            checkScatteredOutput(
                hasNulls,
                i,
                scatter.data(),
                buffer.data(),
                data.data(),
                nullsPtr,
                expectedRow);
          }
          ASSERT_EQ(actualRows, expectedRow);

          const int secondBlock = rowCount - firstBlock;
          const int secondScatterSize = scatter.size() - firstScatterSize;
          expectedRow = actualRows;
          {
            alpha::bits::Bitmap scatterBitmap(scatterPtr, newRowCount);
            actualRows = encoding->materializeNullable(
                secondBlock,
                buffer.data(),
                [&]() { return nullsPtr; },
                &scatterBitmap,
                firstScatterSize);
          }
          ASSERT_EQ(actualRows, secondBlock);
          hasNulls = actualRows != secondScatterSize;
          auto previousRows = expectedRow;
          for (int i = firstScatterSize; i < newRowCount; ++i) {
            checkScatteredOutput(
                hasNulls,
                i,
                scatter.data(),
                buffer.data(),
                data.data(),
                nullsPtr,
                expectedRow);
          }
          ASSERT_EQ(actualRows, expectedRow - previousRows);
          ASSERT_EQ(rowCount, expectedRow);

          encoding->reset();
          expectedRow = 0;
          for (int i = 0; i < rowCount; ++i) {
            // Note: Internally, some bit implementations use word boundaries to
            // efficiently iterate on bitmaps. If the buffer doesn't end on a
            // word boundary, this leads to ASAN buffer overflow (debug builds).
            // So for now, we are using uint64_t as the bitmap to make sure the
            // buffer ends on a word boundary.
            auto scatterStart = scatterSizes[i];
            auto scatterEnd = scatterSizes[i + 1];
            {
              alpha::bits::Bitmap scatterBitmap(scatterPtr, scatterEnd);
              actualRows = encoding->materializeNullable(
                  1,
                  buffer.data(),
                  [&]() { return nullsPtr; },
                  &scatterBitmap,
                  scatterStart);
            }
            ASSERT_EQ(actualRows, 1);
            previousRows = expectedRow;
            for (int j = scatterStart; j < scatterEnd; ++j) {
              checkScatteredOutput(
                  scatterEnd - scatterStart > 1,
                  j,
                  scatter.data(),
                  buffer.data(),
                  data.data(),
                  nullsPtr,
                  expectedRow);
            }
            ASSERT_EQ(actualRows, expectedRow - previousRows);
          }
          ASSERT_EQ(rowCount, expectedRow);

          encoding->reset();
          expectedRow = 0;
          int start = 0;
          int len = 0;
          for (int i = 0; i < rowCount; ++i) {
            start += len;
            len += 1;
            if (start + len > rowCount) {
              break;
            }
            auto scatterStart = scatterSizes[start];
            auto scatterEnd = scatterSizes[start + len];
            {
              alpha::bits::Bitmap scatterBitmap(scatterPtr, scatterEnd);
              actualRows = encoding->materializeNullable(
                  len,
                  buffer.data(),
                  [&]() { return nullsPtr; },
                  &scatterBitmap,
                  scatterStart);
            }
            ASSERT_EQ(actualRows, len);
            hasNulls = actualRows != scatterEnd - scatterStart;
            previousRows = expectedRow;
            for (int j = scatterStart; j < scatterEnd; ++j) {
              checkScatteredOutput(
                  hasNulls,
                  j,
                  scatter.data(),
                  buffer.data(),
                  data.data(),
                  nullsPtr,
                  expectedRow);
            }
            ASSERT_EQ(actualRows, expectedRow - previousRows);
          }
        }
      }
    }
  }
}
