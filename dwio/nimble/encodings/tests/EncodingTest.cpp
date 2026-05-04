/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include "dwio/nimble/encodings/Encoding.h"
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <set>
#include <span>
#include <vector>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DeltaEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "folly/Random.h"
#include "velox/common/base/BitUtil.h"
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

template <typename EncodingType, bool UseVarint>
struct TestConfig {
  using encoding_type = EncodingType;
  static constexpr bool useVarint = UseVarint;
};

#define TC(T) TestConfig<T, false>, TestConfig<T, true>

template <typename Config>
class EncodingTest : public ::testing::Test {
 protected:
  using C = typename Config::encoding_type;
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
      bool useVariableBitWidthCompressor,
      std::function<void*(uint32_t)> stringBufferFactory) {
    constexpr bool useVarint = Config::useVarint;
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

    nimble::Encoding::Options options{.useVarintRowCount = useVarint};
    auto encoded = C::encode(selection, physicalValues, *buffer_, options);
    return std::make_unique<C>(
        *this->pool_, encoded, stringBufferFactory, options);
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

#define VARINT_TYPES(EncodingName)                            \
  TC(EncodingName<int32_t>), TC(EncodingName<int64_t>),       \
      TC(EncodingName<uint32_t>), TC(EncodingName<uint64_t>), \
      TC(EncodingName<float>), TC(EncodingName<double>)

#define NUMERIC_TYPES(EncodingName)                         \
  VARINT_TYPES(EncodingName), TC(EncodingName<int8_t>),     \
      TC(EncodingName<uint8_t>), TC(EncodingName<int16_t>), \
      TC(EncodingName<uint16_t>)

#define NON_BOOL_TYPES(EncodingName) \
  NUMERIC_TYPES(EncodingName), TC(EncodingName<std::string_view>)

#define ALL_TYPES(EncodingName) \
  NON_BOOL_TYPES(EncodingName), TC(EncodingName<bool>)

using TestTypes = ::testing::Types<
    TC(nimble::SparseBoolEncoding),
    VARINT_TYPES(nimble::VarintEncoding),
    NUMERIC_TYPES(nimble::FixedBitWidthEncoding),
    NON_BOOL_TYPES(nimble::DictionaryEncoding),
    NON_BOOL_TYPES(nimble::MainlyConstantEncoding),
    ALL_TYPES(nimble::TrivialEncoding),
    ALL_TYPES(nimble::RLEEncoding),
    ALL_TYPES(nimble::ConstantEncoding)>;

TYPED_TEST_CASE(EncodingTest, TestTypes);

// Regression test for SparseBoolEncoding null pointer crash when rowCount == 0.
// memset(nullptr, 0, 0) is undefined behavior per the C/C++ standard.
class SparseBoolZeroRowTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  std::function<void*(uint32_t)> makeStringBufferFactory() {
    return [this](uint32_t totalLength) {
      auto& buf = stringBuffers_.emplace_back(
          velox::AlignedBuffer::allocate<char>(totalLength, pool_.get()));
      return buf->asMutable<void>();
    };
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};

TEST_F(SparseBoolZeroRowTest, materializeZeroRows) {
  // Create encoding with sparse true values (few trues among mostly false).
  nimble::Vector<bool> values{pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(i == 50);
  }

  auto encoding = facebook::nimble::test::Encoder<nimble::SparseBoolEncoding>::
      createEncoding(*buffer_, values, makeStringBufferFactory());

  // Materializing zero rows with nullptr must not crash.
  encoding->materialize(0, nullptr);

  // Encoding should still work correctly after zero-row materialize.
  nimble::Vector<bool> result(pool_.get(), 100);
  encoding->materialize(100, result.data());
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(result[i], values[i]) << "i: " << i;
  }
}

TEST_F(SparseBoolZeroRowTest, materializeZeroRowsMidStream) {
  nimble::Vector<bool> values{pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(i == 50);
  }

  auto encoding = facebook::nimble::test::Encoder<nimble::SparseBoolEncoding>::
      createEncoding(*buffer_, values, makeStringBufferFactory());

  // Read some rows, then do a zero-row materialize, then continue reading.
  nimble::Vector<bool> result(pool_.get(), 100);
  encoding->materialize(30, result.data());
  encoding->materialize(0, nullptr);
  encoding->materialize(70, result.data());
  for (int i = 0; i < 70; ++i) {
    ASSERT_EQ(result[i], values[30 + i]) << "i: " << i;
  }
}

TEST_F(SparseBoolZeroRowTest, materializeBoolsAsBitsZeroRows) {
  nimble::Vector<bool> values{pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(i == 50);
  }

  auto encoding = facebook::nimble::test::Encoder<nimble::SparseBoolEncoding>::
      createEncoding(*buffer_, values, makeStringBufferFactory());
  auto* sparseBool = dynamic_cast<nimble::SparseBoolEncoding*>(encoding.get());
  ASSERT_NE(sparseBool, nullptr);

  // materializeBoolsAsBits with zero rows and nullptr must not crash.
  sparseBool->materializeBoolsAsBits(0, nullptr, 0);

  // Should still work correctly after the zero-row call.
  uint64_t bits[2] = {};
  sparseBool->materializeBoolsAsBits(100, bits, 0);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(velox::bits::isBitSet(bits, i), values[i]) << "i: " << i;
  }
}

TEST_F(SparseBoolZeroRowTest, materializeZeroRowsSparseValueFalse) {
  // Create encoding with sparse false values (few falses among mostly true).
  nimble::Vector<bool> values{pool_.get()};
  for (int i = 0; i < 100; ++i) {
    values.push_back(i != 50);
  }

  auto encoding = facebook::nimble::test::Encoder<nimble::SparseBoolEncoding>::
      createEncoding(*buffer_, values, makeStringBufferFactory());

  // Materializing zero rows with nullptr must not crash.
  encoding->materialize(0, nullptr);

  nimble::Vector<bool> result(pool_.get(), 100);
  encoding->materialize(100, result.data());
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(result[i], values[i]) << "i: " << i;
  }
}

TYPED_TEST(EncodingTest, materialize) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  using E = typename TypeParam::encoding_type::cppDataType;

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<nimble::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto compress : {false, true}) {
        for (auto useVariableBitWidthCompressor : {false, true}) {
          SCOPED_TRACE(
              fmt::format(
                  "compress {}, variableBitWidth {}",
                  compress,
                  useVariableBitWidthCompressor));
          const int rowCount = data.size();
          ASSERT_GT(rowCount, 0);
          std::unique_ptr<nimble::Encoding> encoding;
          std::vector<velox::BufferPtr> newStringBuffers;
          const auto stringBufferFactory = [&](uint32_t totalLength) {
            auto& buffer = newStringBuffers.emplace_back(
                velox::AlignedBuffer::allocate<char>(
                    totalLength, this->pool_.get()));
            return buffer->template asMutable<void>();
          };
          try {
            encoding = this->createEncoding(
                data,
                compress,
                useVariableBitWidthCompressor,
                stringBufferFactory);
          } catch (const nimble::NimbleUserError& e) {
            if (e.errorCode() == nimble::error_code::IncompatibleEncoding) {
              continue;
            }
            throw;
          }
          ASSERT_EQ(encoding->dataType(), nimble::TypeTraits<E>::dataType);
          ASSERT_EQ(encoding->rowCount(), rowCount);
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
    ASSERT_EQ(
        scatter[index],
        velox::bits::isBitSet(reinterpret_cast<const uint8_t*>(nulls), index));
  }
}

TYPED_TEST(EncodingTest, scatteredMaterialize) {
  using E = typename TypeParam::encoding_type::cppDataType;

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
          std::vector<velox::BufferPtr> newStringBuffers;
          const auto stringBufferFactory = [&](uint32_t totalLength) {
            auto& buffer = newStringBuffers.emplace_back(
                velox::AlignedBuffer::allocate<char>(
                    totalLength, this->pool_.get()));
            return buffer->template asMutable<void>();
          };
          try {
            encoding = this->createEncoding(
                data,
                compress,
                useVariableBitWidthCompressor,
                stringBufferFactory);
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
          auto requiredBytes = velox::bits::nbytes(newRowCount);
          // Note: Internally, some bit implementations use word boundaries to
          // efficiently iterate on bitmaps. If the buffer doesn't end on a
          // word boundary, this leads to ASAN buffer overflow (debug builds).
          // So for now, we are allocating extra 7 bytes to make sure the
          // buffer ends or exceeds a word boundary.
          nimble::Buffer scatterBuffer{*this->pool_, requiredBytes + 7};
          nimble::Buffer nullsBuffer{*this->pool_, requiredBytes + 7};
          auto scatterPtr = scatterBuffer.reserve(requiredBytes);
          auto nullsPtr = nullsBuffer.reserve(requiredBytes);
          memset(scatterPtr, 0, requiredBytes);
          velox::bits::packBitmap(scatter, scatterPtr);

          nimble::Vector<E> buffer(this->pool_.get(), newRowCount);

          uint32_t expectedRow = 0;
          uint32_t actualRows = 0;
          {
            velox::bits::Bitmap scatterBitmap(
                scatterPtr, static_cast<uint32_t>(newRowCount));
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
            velox::bits::Bitmap scatterBitmap(scatterPtr, firstScatterSize);
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
            velox::bits::Bitmap scatterBitmap(
                scatterPtr, static_cast<uint32_t>(newRowCount));
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
            // Note: Internally, some bit implementations use word boundaries
            // to efficiently iterate on bitmaps. If the buffer doesn't end on
            // a word boundary, this leads to ASAN buffer overflow (debug
            // builds). So for now, we are using uint64_t as the bitmap to
            // make sure the buffer ends on a word boundary.
            auto scatterStart = scatterSizes[i];
            auto scatterEnd = scatterSizes[i + 1];
            {
              velox::bits::Bitmap scatterBitmap(scatterPtr, scatterEnd);
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
              velox::bits::Bitmap scatterBitmap(scatterPtr, scatterEnd);
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

// Templatized dictionary API tests covering all DictionaryEncoding-compatible
// types: numeric types + string_view + bool.
template <typename T>
class DictionaryApiTypedTest : public ::testing::Test {
 protected:
  using PhysicalType = typename nimble::TypeTraits<T>::physicalType;

  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  std::unique_ptr<nimble::Encoding> encodeDictionary(
      const std::vector<T>& values) {
    nimble::Vector<T> vec{pool_.get()};
    for (auto v : values) {
      vec.push_back(v);
    }
    auto physicalValues = std::span<const PhysicalType>(
        reinterpret_cast<const PhysicalType*>(vec.data()), vec.size());
    nimble::EncodingSelection<PhysicalType> selection{
        {.encodingType = nimble::EncodingType::Dictionary},
        nimble::Statistics<PhysicalType>::create(physicalValues),
        std::make_unique<TestTrivialEncodingSelectionPolicy<T>>(false, false)};
    auto encoded = nimble::DictionaryEncoding<T>::encode(
        selection, physicalValues, *buffer_);
    return std::make_unique<nimble::DictionaryEncoding<T>>(
        *pool_, encoded, [this](uint32_t size) {
          stringBuffers_.push_back(
              velox::AlignedBuffer::allocate<char>(size, pool_.get()));
          return stringBuffers_.back()->asMutable<void>();
        });
  }

  std::unique_ptr<nimble::Encoding> encodeNullableDictionary(
      const std::vector<T>& values,
      const nimble::Vector<bool>& nulls) {
    NIMBLE_CHECK_EQ(values.size(), nulls.size());
    const uint32_t rowCount = values.size();

    // Encode non-null values as DictionaryEncoding.
    nimble::Vector<T> nonNullVec{pool_.get()};
    for (size_t i = 0; i < values.size(); ++i) {
      if (nulls[i]) {
        nonNullVec.push_back(values[i]);
      }
    }
    auto nonNullSpan = std::span<const PhysicalType>(
        reinterpret_cast<const PhysicalType*>(nonNullVec.data()),
        nonNullVec.size());
    nimble::Buffer valBuffer{*pool_};
    nimble::EncodingSelection<PhysicalType> valSelection{
        {.encodingType = nimble::EncodingType::Dictionary},
        nimble::Statistics<PhysicalType>::create(nonNullSpan),
        std::make_unique<TestTrivialEncodingSelectionPolicy<T>>(false, false)};
    auto serializedValues = nimble::DictionaryEncoding<T>::encode(
        valSelection, nonNullSpan, valBuffer);

    // Encode nulls as TrivialEncoding<bool>.
    auto nullSpan = std::span<const bool>(nulls.data(), nulls.size());
    nimble::Buffer nullBuffer{*pool_};
    nimble::EncodingSelection<bool> nullSelection{
        {.encodingType = nimble::EncodingType::Trivial},
        nimble::Statistics<bool>::create(nullSpan),
        std::make_unique<TestTrivialEncodingSelectionPolicy<bool>>(
            false, false)};
    auto serializedNulls = nimble::TrivialEncoding<bool>::encode(
        nullSelection, nullSpan, nullBuffer);

    // Assemble the NullableEncoding binary format:
    // [EncodingType:1][DataType:1][rowCount:4] | uint32(valSize) | valBytes |
    // nullBytes
    const uint32_t encodingSize = nimble::Encoding::kPrefixSize + 4 +
        serializedValues.size() + serializedNulls.size();
    char* reserved = buffer_->reserve(encodingSize);
    char* pos = reserved;
    nimble::encoding::writeChar(
        static_cast<char>(nimble::EncodingType::Nullable), pos);
    nimble::encoding::writeChar(
        static_cast<char>(nimble::TypeTraits<T>::dataType), pos);
    nimble::encoding::writeUint32(rowCount, pos);
    nimble::encoding::writeString(serializedValues, pos);
    nimble::encoding::writeBytes(serializedNulls, pos);

    return nimble::EncodingFactory().create(
        *pool_,
        std::string_view(reserved, encodingSize),
        [this](uint32_t size) {
          stringBuffers_.push_back(
              velox::AlignedBuffer::allocate<char>(size, pool_.get()));
          return stringBuffers_.back()->asMutable<void>();
        });
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
  std::vector<std::string> stringPool_;
};

using DictionaryApiTypes = ::testing::Types<
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    float,
    double,
    std::string_view>;

TYPED_TEST_SUITE(DictionaryApiTypedTest, DictionaryApiTypes);

TYPED_TEST(DictionaryApiTypedTest, basics) {
  using T = TypeParam;
  std::vector<T> data;
  if constexpr (std::is_same_v<T, std::string_view>) {
    data = {"alpha", "bravo", "alpha", "charlie", "bravo"};
  } else {
    data = {T(1), T(2), T(1), T(3), T(2)};
  }

  auto encoding = this->encodeDictionary(data);
  EXPECT_TRUE(encoding->dictionaryEnabled());
  EXPECT_EQ(encoding->dictionarySize(), 3);

  const auto* entries = static_cast<const T*>(encoding->dictionaryEntries());
  ASSERT_NE(entries, nullptr);
  std::set<T> actualUniques(entries, entries + 3);

  if constexpr (std::is_same_v<T, std::string_view>) {
    EXPECT_THAT(
        actualUniques,
        ::testing::UnorderedElementsAre("alpha", "bravo", "charlie"));
  } else {
    EXPECT_THAT(
        actualUniques, ::testing::UnorderedElementsAre(T(1), T(2), T(3)));
  }

  for (uint32_t i = 0; i < 3; ++i) {
    const auto* entry = static_cast<const T*>(encoding->dictionaryEntry(i));
    EXPECT_EQ(*entry, entries[i]);
  }
}

TYPED_TEST(DictionaryApiTypedTest, fuzz) {
  using T = TypeParam;
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  constexpr int kNumRuns = 10;
  constexpr int kMaxRows = 300;

  for (int run = 0; run < kNumRuns; ++run) {
    const int numRows = 2 + rng() % kMaxRows;
    const int cardinality = 2 + rng() % 10;

    // For string types, build a pool of unique random strings per run.
    if constexpr (std::is_same_v<T, std::string_view>) {
      this->stringPool_.clear();
      for (int j = 0; j < cardinality; ++j) {
        const int len = rng() % 30;
        std::string s = fmt::format("{}:", j);
        for (int k = 0; k < len; ++k) {
          s += static_cast<char>('a' + rng() % 26);
        }
        this->stringPool_.push_back(std::move(s));
      }
    }

    std::vector<T> data;
    std::set<T> expectedUniques;
    data.reserve(numRows);
    for (int i = 0; i < numRows; ++i) {
      T val;
      if constexpr (std::is_same_v<T, std::string_view>) {
        val = std::string_view(this->stringPool_[rng() % cardinality]);
      } else {
        val = static_cast<T>(rng() % cardinality);
      }
      data.push_back(val);
      expectedUniques.insert(val);
    }

    SCOPED_TRACE(fmt::format("run={} seed={} numRows={}", run, seed, numRows));

    auto encoding = this->encodeDictionary(data);
    ASSERT_TRUE(encoding->dictionaryEnabled());
    ASSERT_EQ(encoding->dictionarySize(), expectedUniques.size());

    const auto* entries = static_cast<const T*>(encoding->dictionaryEntries());
    std::set<T> actualUniques(entries, entries + encoding->dictionarySize());
    EXPECT_EQ(actualUniques, expectedUniques);

    for (uint32_t i = 0; i < encoding->dictionarySize(); ++i) {
      const auto* entry = static_cast<const T*>(encoding->dictionaryEntry(i));
      EXPECT_EQ(*entry, entries[i]);
    }
  }
}

TYPED_TEST(DictionaryApiTypedTest, nullableBasics) {
  using T = TypeParam;
  std::vector<T> data;
  if constexpr (std::is_same_v<T, std::string_view>) {
    data = {"alpha", "bravo", "alpha", "charlie", "bravo"};
  } else {
    data = {T(1), T(2), T(1), T(3), T(2)};
  }
  nimble::Vector<bool> nulls{this->pool_.get()};
  for (bool b : {true, true, false, true, true}) {
    nulls.push_back(b);
  }

  auto encoding = this->encodeNullableDictionary(data, nulls);
  ASSERT_TRUE(encoding->isNullable());
  EXPECT_TRUE(encoding->dictionaryEnabled());
  EXPECT_EQ(encoding->dictionarySize(), 3);

  const auto* entries = static_cast<const T*>(encoding->dictionaryEntries());
  ASSERT_NE(entries, nullptr);
  std::set<T> actualUniques(entries, entries + 3);

  if constexpr (std::is_same_v<T, std::string_view>) {
    EXPECT_THAT(
        actualUniques,
        ::testing::UnorderedElementsAre("alpha", "bravo", "charlie"));
  } else {
    EXPECT_THAT(
        actualUniques, ::testing::UnorderedElementsAre(T(1), T(2), T(3)));
  }

  for (uint32_t i = 0; i < 3; ++i) {
    const auto* entry = static_cast<const T*>(encoding->dictionaryEntry(i));
    EXPECT_EQ(*entry, entries[i]);
  }
}

TYPED_TEST(DictionaryApiTypedTest, nullableFuzz) {
  using T = TypeParam;
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  constexpr int kNumRuns = 10;
  constexpr int kMaxRows = 300;

  for (int run = 0; run < kNumRuns; ++run) {
    const int numRows = 2 + rng() % kMaxRows;
    const int cardinality = 2 + rng() % 10;

    if constexpr (std::is_same_v<T, std::string_view>) {
      this->stringPool_.clear();
      for (int j = 0; j < cardinality; ++j) {
        const int len = rng() % 30;
        std::string s = fmt::format("{}:", j);
        for (int k = 0; k < len; ++k) {
          s += static_cast<char>('a' + rng() % 26);
        }
        this->stringPool_.push_back(std::move(s));
      }
    }

    std::vector<T> data;
    nimble::Vector<bool> nulls{this->pool_.get()};
    std::set<T> expectedUniques;
    data.reserve(numRows);
    nulls.reserve(numRows);
    for (int i = 0; i < numRows; ++i) {
      bool isNull = (rng() % 5 == 0);
      nulls.push_back(!isNull);
      T val;
      if constexpr (std::is_same_v<T, std::string_view>) {
        val = std::string_view(this->stringPool_[rng() % cardinality]);
      } else {
        val = static_cast<T>(rng() % cardinality);
      }
      data.push_back(val);
      if (!isNull) {
        expectedUniques.insert(val);
      }
    }

    if (expectedUniques.empty()) {
      continue;
    }

    SCOPED_TRACE(fmt::format("run={} seed={} numRows={}", run, seed, numRows));

    auto encoding = this->encodeNullableDictionary(data, nulls);
    ASSERT_TRUE(encoding->isNullable());
    ASSERT_TRUE(encoding->dictionaryEnabled());
    ASSERT_EQ(encoding->dictionarySize(), expectedUniques.size());

    const auto* entries = static_cast<const T*>(encoding->dictionaryEntries());
    std::set<T> actualUniques(entries, entries + encoding->dictionarySize());
    EXPECT_EQ(actualUniques, expectedUniques);

    for (uint32_t i = 0; i < encoding->dictionarySize(); ++i) {
      const auto* entry = static_cast<const T*>(encoding->dictionaryEntry(i));
      EXPECT_EQ(*entry, entries[i]);
    }
  }
}

// Non-templated test for unsupported encodings and bool dictionary.
class DictionaryApiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::vector<velox::BufferPtr> stringBuffers_;
};

// Verifies that every non-dictionary encoding type returns
// dictionaryEnabled()=false and aborts on dictionary-specific methods.
TEST_F(DictionaryApiTest, unsupportedEncodings) {
  auto verifyUnsupported = [](nimble::Encoding& enc, const char* name) {
    SCOPED_TRACE(name);
    EXPECT_FALSE(enc.dictionaryEnabled());
    EXPECT_THROW(enc.dictionarySize(), nimble::NimbleInternalError);
    EXPECT_THROW(enc.dictionaryEntry(0), nimble::NimbleInternalError);
    EXPECT_THROW(enc.dictionaryEntries(), nimble::NimbleInternalError);
  };

  // Trivial<string_view>
  {
    nimble::Vector<std::string_view> vals{pool_.get()};
    vals.push_back("a");
    vals.push_back("b");
    auto span = std::span<const std::string_view>(vals.data(), vals.size());
    nimble::EncodingSelection<std::string_view> sel{
        {.encodingType = nimble::EncodingType::Trivial},
        nimble::Statistics<std::string_view>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<std::string_view>>(
            false, false)};
    auto encoded =
        nimble::TrivialEncoding<std::string_view>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(
        *pool_, encoded, [this](uint32_t size) {
          stringBuffers_.push_back(
              velox::AlignedBuffer::allocate<char>(size, pool_.get()));
          return stringBuffers_.back()->asMutable<void>();
        });
    verifyUnsupported(*enc, "Trivial");
  }

  // Constant<uint32_t>
  {
    std::vector<uint32_t> data(10, 42);
    auto span = std::span<const uint32_t>(data);
    nimble::EncodingSelection<uint32_t> sel{
        {.encodingType = nimble::EncodingType::Constant},
        nimble::Statistics<uint32_t>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<uint32_t>>(
            false, false)};
    auto encoded =
        nimble::ConstantEncoding<uint32_t>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "Constant");
  }

  // RLE<uint32_t>
  {
    std::vector<uint32_t> data(10, 5);
    auto span = std::span<const uint32_t>(data);
    nimble::EncodingSelection<uint32_t> sel{
        {.encodingType = nimble::EncodingType::RLE},
        nimble::Statistics<uint32_t>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<uint32_t>>(
            false, false)};
    auto encoded = nimble::RLEEncoding<uint32_t>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "RLE");
  }

  // FixedBitWidth<uint32_t>
  {
    std::vector<uint32_t> data = {1, 3, 7, 15};
    auto span = std::span<const uint32_t>(data);
    nimble::EncodingSelection<uint32_t> sel{
        {.encodingType = nimble::EncodingType::FixedBitWidth},
        nimble::Statistics<uint32_t>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<uint32_t>>(
            false, false)};
    auto encoded =
        nimble::FixedBitWidthEncoding<uint32_t>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "FixedBitWidth");
  }

  // MainlyConstant<uint32_t>
  {
    std::vector<uint32_t> data(10, 99);
    data.push_back(1);
    auto span = std::span<const uint32_t>(data);
    nimble::EncodingSelection<uint32_t> sel{
        {.encodingType = nimble::EncodingType::MainlyConstant},
        nimble::Statistics<uint32_t>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<uint32_t>>(
            false, false)};
    auto encoded =
        nimble::MainlyConstantEncoding<uint32_t>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "MainlyConstant");
  }

  // SparseBool
  {
    nimble::Vector<bool> vals{pool_.get()};
    for (int i = 0; i < 10; ++i) {
      vals.push_back(i != 5);
    }
    auto span = std::span<const bool>(vals.data(), vals.size());
    nimble::EncodingSelection<bool> sel{
        {.encodingType = nimble::EncodingType::SparseBool},
        nimble::Statistics<bool>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<bool>>(
            false, false)};
    auto encoded = nimble::SparseBoolEncoding::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "SparseBool");
  }

  // Varint<uint64_t>
  {
    std::vector<uint64_t> data = {100, 200, 300};
    auto span = std::span<const uint64_t>(data);
    nimble::EncodingSelection<uint64_t> sel{
        {.encodingType = nimble::EncodingType::Varint},
        nimble::Statistics<uint64_t>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<uint64_t>>(
            false, false)};
    auto encoded =
        nimble::VarintEncoding<uint64_t>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "Varint");
  }

  // Delta<uint32_t>
  {
    std::vector<uint32_t> data = {10, 20, 30};
    auto span = std::span<const uint32_t>(data);
    nimble::EncodingSelection<uint32_t> sel{
        {.encodingType = nimble::EncodingType::Delta},
        nimble::Statistics<uint32_t>::create(span),
        std::make_unique<TestTrivialEncodingSelectionPolicy<uint32_t>>(
            false, false)};
    auto encoded = nimble::DeltaEncoding<uint32_t>::encode(sel, span, *buffer_);
    auto enc = nimble::EncodingFactory().create(*pool_, encoded, nullptr);
    verifyUnsupported(*enc, "Delta");
  }
}
