/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <vector>
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DeltaEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"
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

using namespace facebook;

namespace {

class TestCompressPolicy : public nimble::CompressionPolicy {
 public:
  explicit TestCompressPolicy(bool compress, bool useVariableBitWidthCompressor)
      : compress_{compress},
        useVariableBitWidthCompressor_{useVariableBitWidthCompressor} {}

  nimble::CompressionInformation compression() const override {
    if (!compress_) {
      return {.compressionType = nimble::CompressionType::Uncompressed};
    }

    nimble::CompressionInformation information{
        .compressionType = nimble::CompressionType::MetaInternal};
    information.parameters.metaInternal.compressionLevel = 9;
    information.parameters.metaInternal.decompressionLevel = 2;
    information.parameters.metaInternal.useVariableBitWidthCompressor =
        useVariableBitWidthCompressor_;
    return information;
  }

  virtual bool shouldAccept(
      nimble::CompressionType /* compressionType */,
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
    : public nimble::EncodingSelectionPolicy<T> {
  using physicalType = typename nimble::TypeTraits<T>::physicalType;

 public:
  explicit TestTrivialEncodingSelectionPolicy(
      bool shouldCompress,
      bool useVariableBitWidthCompressor)
      : shouldCompress_{shouldCompress},
        useVariableBitWidthCompressor_{useVariableBitWidthCompressor} {}

  nimble::EncodingSelectionResult select(
      std::span<const physicalType> /* values */,
      const nimble::Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = nimble::EncodingType::Trivial,
        .compressionPolicyFactory = [this]() {
          return std::make_unique<TestCompressPolicy>(
              shouldCompress_, useVariableBitWidthCompressor_);
        }};
  }

  nimble::EncodingSelectionResult selectNullable(
      std::span<const physicalType> /* values */,
      std::span<const bool> /* nulls */,
      const nimble::Statistics<physicalType>& /* statistics */) override {
    return {
        .encodingType = nimble::EncodingType::Nullable,
    };
  }

  std::unique_ptr<nimble::EncodingSelectionPolicyBase> createImpl(
      nimble::EncodingType /* encodingType */,
      nimble::NestedEncodingIdentifier /* identifier */,
      nimble::DataType type) override {
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
    buffer_ = std::make_unique<nimble::Buffer>(*this->pool_);
    util_ = std::make_unique<nimble::testing::Util>(*this->pool_);
  }

  template <typename Encoding>
  struct EncodingTypeTraits {};

  template <>
  struct EncodingTypeTraits<nimble::ConstantEncoding<E>> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::Constant;
  };

  template <>
  struct EncodingTypeTraits<nimble::DictionaryEncoding<E>> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::Dictionary;
  };

  template <>
  struct EncodingTypeTraits<nimble::FixedBitWidthEncoding<E>> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::FixedBitWidth;
  };

  template <>
  struct EncodingTypeTraits<nimble::MainlyConstantEncoding<E>> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::MainlyConstant;
  };

  template <>
  struct EncodingTypeTraits<nimble::RLEEncoding<E>> {
    static inline nimble::EncodingType encodingType = nimble::EncodingType::RLE;
  };

  template <>
  struct EncodingTypeTraits<nimble::SparseBoolEncoding> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::SparseBool;
  };

  template <>
  struct EncodingTypeTraits<nimble::TrivialEncoding<E>> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::Trivial;
  };

  template <>
  struct EncodingTypeTraits<nimble::VarintEncoding<E>> {
    static inline nimble::EncodingType encodingType =
        nimble::EncodingType::Varint;
  };

  std::unique_ptr<nimble::Encoding> createEncoding(
      const nimble::Vector<E>& values,
      bool compress,
      bool useVariableBitWidthCompressor) {
    using physicalType = typename nimble::TypeTraits<E>::physicalType;
    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    nimble::EncodingSelection<physicalType> selection{
        {.encodingType = EncodingTypeTraits<C>::encodingType,
         .compressionPolicyFactory =
             [compress, useVariableBitWidthCompressor]() {
               return std::make_unique<TestCompressPolicy>(
                   compress, useVariableBitWidthCompressor);
             }},
        nimble::Statistics<physicalType>::create(physicalValues),
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
  std::unique_ptr<nimble::Buffer> buffer_;
  std::unique_ptr<nimble::testing::Util> util_;
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
    nimble::SparseBoolEncoding,
    VARINT_TYPES(nimble::VarintEncoding),
    NUMERIC_TYPES(nimble::FixedBitWidthEncoding),
    NON_BOOL_TYPES(nimble::DictionaryEncoding),
    NON_BOOL_TYPES(nimble::MainlyConstantEncoding),
    ALL_TYPES(nimble::TrivialEncoding),
    ALL_TYPES(nimble::RLEEncoding),
    ALL_TYPES(nimble::ConstantEncoding)>;

TYPED_TEST_CASE(EncodingTests, TestTypes);

TYPED_TEST(EncodingTests, Materialize) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  using E = typename TypeParam::cppDataType;

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<nimble::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto compress : {false, true}) {
        for (auto useVariableBitWidthCompressor : {false, true}) {
          const int rowCount = data.size();
          ASSERT_GT(rowCount, 0);
          std::unique_ptr<nimble::Encoding> encoding;
          try {
            encoding = this->createEncoding(
                data, compress, useVariableBitWidthCompressor);
          } catch (const nimble::NimbleUserError& e) {
            if (e.errorCode() == nimble::error_code::IncompatibleEncoding) {
              continue;
            }
            throw;
          }
          ASSERT_EQ(encoding->dataType(), nimble::TypeTraits<E>::dataType);
          nimble::Vector<E> buffer(this->pool_.get(), rowCount);

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
    ASSERT_EQ(scatter[index], nimble::bits::getBit(index, nulls));
  }
}

TYPED_TEST(EncodingTests, ScatteredMaterialize) {
  using E = typename TypeParam::cppDataType;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<nimble::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto compress : {false, true}) {
        for (auto useVariableBitWidthCompressor : {false, true}) {
          const int rowCount = data.size();
          ASSERT_GT(rowCount, 0);
          std::unique_ptr<nimble::Encoding> encoding;
          try {
            encoding = this->createEncoding(
                data, compress, useVariableBitWidthCompressor);
          } catch (const nimble::NimbleUserError& e) {
            if (e.errorCode() == nimble::error_code::IncompatibleEncoding) {
              continue;
            }
            throw;
          }
          ASSERT_EQ(encoding->dataType(), nimble::TypeTraits<E>::dataType);

          int setBits = 0;
          std::vector<int32_t> scatterSizes(rowCount + 1);
          scatterSizes[0] = 0;
          nimble::Vector<bool> scatter(this->pool_.get());
          while (setBits < rowCount) {
            scatter.push_back(folly::Random::oneIn(2, rng));
            if (scatter.back()) {
              scatterSizes[++setBits] = scatter.size();
            }
          }

          auto newRowCount = scatter.size();
          auto requiredBytes = nimble::bits::bytesRequired(newRowCount);
          // Note: Internally, some bit implementations use word boundaries to
          // efficiently iterate on bitmaps. If the buffer doesn't end on a word
          // boundary, this leads to ASAN buffer overflow (debug builds). So for
          // now, we are allocating extra 7 bytes to make sure the buffer ends
          // or exceeds a word boundary.
          nimble::Buffer scatterBuffer{*this->pool_, requiredBytes + 7};
          nimble::Buffer nullsBuffer{*this->pool_, requiredBytes + 7};
          auto scatterPtr = scatterBuffer.reserve(requiredBytes);
          auto nullsPtr = nullsBuffer.reserve(requiredBytes);
          memset(scatterPtr, 0, requiredBytes);
          nimble::bits::packBitmap(scatter, scatterPtr);

          nimble::Vector<E> buffer(this->pool_.get(), newRowCount);

          uint32_t expectedRow = 0;
          uint32_t actualRows = 0;
          {
            nimble::bits::Bitmap scatterBitmap(scatterPtr, newRowCount);
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
            nimble::bits::Bitmap scatterBitmap(scatterPtr, firstScatterSize);
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
            nimble::bits::Bitmap scatterBitmap(scatterPtr, newRowCount);
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
              nimble::bits::Bitmap scatterBitmap(scatterPtr, scatterEnd);
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
              nimble::bits::Bitmap scatterBitmap(scatterPtr, scatterEnd);
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
