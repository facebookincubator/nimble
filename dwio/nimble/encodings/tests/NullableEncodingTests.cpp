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
#include <span>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/common/tests/TestUtils.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactoryNew.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/SentinelEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "folly/Random.h"
#include "velox/common/memory/Memory.h"

// Tests the Encoding API for all nullable Encoding implementations + data
// types.
//
// These encodings generally use the factory themselves to encode their non-null
// values as another encoding. We assume that the other encodings are thoroughly
// tested and conform to the API, so we can use just a single underlying
// encoding implementation for the non-null values (namely, the TrivialEncoding)
// rather than having to test all the numNullableEncodings X numNormalEncodings
// combinations.

using namespace ::facebook;

namespace {
enum class NullsPattern {
  None,
  All,
  Random,
};
}

// C is the encoding type.
template <typename C>
class NullableEncodingTest : public ::testing::Test {
 protected:
  using E = typename C::cppDataType;

  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
    util_ = std::make_unique<nimble::testing::Util>(*pool_);
  }

  // Makes a random-length nulls vector with num_nonNulls values set to true
  // (and at least one value set to false) scattered randomly throughout.
  template <typename RNG>
  nimble::Vector<bool>
  makeRandomNulls(RNG&& rng, uint32_t dataSize, NullsPattern pattern) {
    if (pattern == NullsPattern::None) {
      return nimble::Vector<bool>{pool_.get(), dataSize, false};
    } else if (pattern == NullsPattern::All) {
      return nimble::Vector<bool>{pool_.get(), dataSize, true};
    }
    const uint32_t rowCount =
        dataSize + folly::Random::rand32(3 * kMaxRows, std::forward<RNG>(rng));
    nimble::Vector<bool> nulls(pool_.get(), rowCount, false);
    for (uint32_t i = 0; i < dataSize; ++i) {
      nulls[i] = true;
    }
    std::random_shuffle(nulls.begin(), nulls.end());
    return nulls;
  }

  // Each unit test runs on randomized data this many times before
  // we conclude the unit test passed.
  static constexpr int kNumRandomRuns = 20;
  // We want the number of row tested to potentially be large compared to a
  // skip block. When we actually generate data we pick a random length between
  // 1 and this size.
  static constexpr int kMaxRows = 2000;

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
  std::unique_ptr<nimble::testing::Util> util_;
};

#define ALL_TYPES(EncodingName)                                          \
  EncodingName<int>, EncodingName<int64_t>, EncodingName<uint32_t>,      \
      EncodingName<uint64_t>, EncodingName<float>, EncodingName<double>, \
      EncodingName<std::string_view>, EncodingName<bool>

#define NON_BOOL_TYPES(EncodingName)                                     \
  EncodingName<int>, EncodingName<int64_t>, EncodingName<uint32_t>,      \
      EncodingName<uint64_t>, EncodingName<float>, EncodingName<double>, \
      EncodingName<std::string_view>

using TestTypes = ::testing::Types<
    ALL_TYPES(nimble::NullableEncoding),
    NON_BOOL_TYPES(nimble::SentinelEncoding)>;

TYPED_TEST_CASE(NullableEncodingTest, TestTypes);

//.Spreads the nonNulls out into a vector of length |nulls|, with a non-null
// placed at each true value in |nulls|. Equivalent to Encoding::Materialize.
template <typename E>
nimble::Vector<E> spreadNullsIntoData(
    velox::memory::MemoryPool& memoryPool,
    std::span<const E> nonNulls,
    std::span<const bool> nulls) {
  nimble::Vector<E> result(&memoryPool);
  auto nonNullsIt = nonNulls.begin();
  for (auto nulls_it = nulls.begin(); nulls_it < nulls.end(); ++nulls_it) {
    if (*nulls_it) {
      result.push_back(*nonNullsIt++);
    } else {
      result.push_back(E());
    }
  }

  return result;
}

TYPED_TEST(NullableEncodingTest, Materialize) {
  using E = typename TypeParam::cppDataType;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<nimble::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto nullPattern :
           {NullsPattern::None, NullsPattern::All, NullsPattern::Random}) {
        const nimble::Vector<bool> nulls = this->makeRandomNulls(
            rng, folly::to<uint32_t>(data.size()), nullPattern);
        // Spreading the data out will help us check correctness more easily.
        const nimble::Vector<E> spreadData =
            spreadNullsIntoData<E>(*this->pool_, data, nulls);

        auto encoding = nimble::test::Encoder<nimble::NullableEncoding<E>>::
            createNullableEncoding(*this->buffer_, data, nulls);
        ASSERT_EQ(encoding->dataType(), nimble::TypeTraits<E>::dataType);
        ASSERT_TRUE(encoding->isNullable());
        const uint32_t rowCount = encoding->rowCount();

        nimble::Vector<E> buffer(this->pool_.get(), rowCount);
        encoding->materialize(rowCount, buffer.data());
        for (int i = 0; i < rowCount; ++i) {
          ASSERT_EQ(buffer[i], spreadData[i]);
        }

        encoding->reset();
        const int firstBlock = folly::to<int>(rowCount / 2);
        encoding->materialize(firstBlock, buffer.data());
        for (int i = 0; i < firstBlock; ++i) {
          ASSERT_EQ(buffer[i], spreadData[i]);
        }
        const int secondBlock = rowCount - firstBlock;
        encoding->materialize(secondBlock, buffer.data());
        for (int i = 0; i < secondBlock; ++i) {
          ASSERT_EQ(buffer[i], spreadData[firstBlock + i]);
        }

        encoding->reset();
        for (int i = 0; i < rowCount; ++i) {
          encoding->materialize(1, buffer.data());
          ASSERT_EQ(buffer[0], spreadData[i]);
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
            ASSERT_EQ(spreadData[start + j], buffer[j]);
          }
        }

        const uint32_t offset =
            folly::to<uint32_t>(folly::Random::rand32(rng) % data.size());
        const uint32_t length = folly::to<uint32_t>(
            1 + folly::Random::rand32(rng) % (data.size() - offset));
        encoding->reset();
        encoding->skip(offset);
        encoding->materialize(length, buffer.data());
        for (uint32_t i = 0; i < length; ++i) {
          ASSERT_EQ(buffer[i], spreadData[offset + i]);
        }
      }
    }
  }
}

template <typename T>
void checkOutput(
    size_t index,
    const bool* nulls,
    const T* data,
    const char* actualNulls,
    const T* actualData,
    bool hasNulls) {
  if (nulls[index]) {
    ASSERT_EQ(data[index], actualData[index]) << index;
  }
  if (hasNulls) {
    ASSERT_EQ(nimble::bits::getBit(index, actualNulls), nulls[index]) << index;
  }
}

TYPED_TEST(NullableEncodingTest, ScatteredMaterialize) {
  using E = typename TypeParam::cppDataType;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (int run = 0; run < this->kNumRandomRuns; ++run) {
    const std::vector<nimble::Vector<E>> dataPatterns =
        this->util_->template makeDataPatterns<E>(
            rng, this->kMaxRows, this->buffer_.get());
    for (const auto& data : dataPatterns) {
      for (auto nullPattern :
           {NullsPattern::None, NullsPattern::All, NullsPattern::Random}) {
        const nimble::Vector<bool> nulls =
            this->makeRandomNulls(rng, data.size(), nullPattern);
        // Spreading the data out will help us check correctness more easily.
        const nimble::Vector<E> spreadData =
            spreadNullsIntoData<E>(*this->pool_, data, nulls);

        auto encoding = nimble::test::Encoder<nimble::NullableEncoding<E>>::
            createNullableEncoding(*this->buffer_, data, nulls);
        ASSERT_EQ(encoding->dataType(), nimble::TypeTraits<E>::dataType);
        ASSERT_TRUE(encoding->isNullable());
        const uint32_t rowCount = encoding->rowCount();
        ASSERT_EQ(rowCount, nulls.size());

        int setBits = 0;
        std::vector<int32_t> scatterSizes(rowCount + 1);
        scatterSizes[0] = 0;
        nimble::Vector<bool> scatter(this->pool_.get());
        while (setBits < rowCount) {
          scatter.push_back(folly::Random::rand32(2, rng) ? true : false);
          if (scatter.back()) {
            scatterSizes[++setBits] = scatter.size();
          }
        }

        auto newRowCount = scatter.size();
        auto requiredBytes = nimble::bits::bytesRequired(newRowCount);
        // Note: Internally, some bit implementations use word boundaries to
        // efficiently iterate on bitmaps. If the buffer doesn't end on a word
        // boundary, this leads to ASAN buffer overflow (debug builds). So for
        // now, we are allocating extra 7 bytes to make sure the buffer ends or
        // exceeds a word boundary.
        nimble::Buffer scatterBuffer{*this->pool_, requiredBytes + 7};
        nimble::Buffer nullsBuffer{*this->pool_, requiredBytes + 7};
        auto scatterPtr = scatterBuffer.reserve(requiredBytes);
        auto nullsPtr = nullsBuffer.reserve(requiredBytes);
        memset(scatterPtr, 0, requiredBytes);
        nimble::bits::packBitmap(scatter, scatterPtr);

        nimble::Vector<E> buffer(this->pool_.get(), newRowCount);

        auto test = [&encoding, &scatter, &nulls, &spreadData](
                        uint32_t rowCount,
                        E* buffer,
                        void* nullsBitmap,
                        uint32_t scatterCount,
                        void* scatterBitmap,
                        uint32_t scatterOffset = 0,
                        uint32_t expectedOffset = 0) {
          uint32_t expectedRow = 0;
          nimble::bits::Bitmap bitmap{
              scatterBitmap, scatterOffset + scatterCount};
          auto nonNullCount = encoding->materializeNullable(
              rowCount,
              buffer,
              [&]() { return nullsBitmap; },
              &bitmap,
              scatterOffset);
          for (int i = 0; i < scatterCount; ++i) {
            auto isSet = false;
            if (scatter[i + scatterOffset]) {
              if (nulls[expectedRow + expectedOffset]) {
                ASSERT_EQ(
                    buffer[i + scatterOffset],
                    spreadData[expectedRow + expectedOffset]);
                isSet = true;
              }
              ++expectedRow;
            }
            if (nonNullCount != scatterCount) {
              ASSERT_EQ(
                  isSet,
                  nimble::bits::getBit(
                      i + scatterOffset,
                      reinterpret_cast<const char*>(nullsBitmap)));
            }
          }

          ASSERT_EQ(rowCount, expectedRow);
        };

        // Test reading all data
        test(rowCount, buffer.data(), nullsPtr, newRowCount, scatterPtr);

        encoding->reset();
        const int firstBlock = newRowCount / 2;

        auto firstBlockSetBits =
            nimble::bits::countSetBits(0, firstBlock, scatterPtr);

        // Test reading first half of the data
        test(
            firstBlockSetBits, buffer.data(), nullsPtr, firstBlock, scatterPtr);

        const int secondBlock = newRowCount - firstBlock;

        // Test reading second half of the data
        test(
            nimble::bits::countSetBits(firstBlock, secondBlock, scatterPtr),
            buffer.data(),
            nullsPtr,
            secondBlock,
            scatterPtr,
            /* scatterOffset */ firstBlock,
            /* expectedOffset */ firstBlockSetBits);

        encoding->reset();
        uint32_t expectedRow = 0;
        for (int i = 0; i < rowCount; ++i) {
          // Note: Internally, some bit implementations use word boundaries to
          // efficiently iterate on bitmaps. If the buffer doesn't end on a word
          // boundary, this leads to ASAN buffer overflow (debug builds). So for
          // now, we are using uint64_t as the bitmap to make sure the buffer
          // ends on a word boundary.
          auto scatterStart = scatterSizes[i];
          auto scatterSize = scatterSizes[i + 1] - scatterStart;

          // Test reading one item at a time
          test(
              1,
              buffer.data(),
              nullsPtr,
              scatterSize,
              scatterPtr,
              /* scatterOffset */ scatterStart,
              /* expectedOffset */ expectedRow);

          ++expectedRow;
        }

        encoding->reset();
        expectedRow = 0;
        int start = 0;
        int len = 0;
        while (true) {
          start += len;
          len += 1;
          if (start + len > rowCount) {
            break;
          }
          auto scatterStart = scatterSizes[start];
          auto scatterSize = scatterSizes[start + len] - scatterStart;

          // Test reading different ranges of data
          test(
              len,
              buffer.data(),
              nullsPtr,
              scatterSize,
              scatterPtr,
              /* scatterOffset */ scatterStart,
              /* expectedOffset */ expectedRow);

          expectedRow += len;
        }
      }
    }
  }
}

TYPED_TEST(NullableEncodingTest, MaterializeNullable) {
  using E = typename TypeParam::cppDataType;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  for (auto nullPattern :
       {NullsPattern::None, NullsPattern::All, NullsPattern::Random}) {
    for (int run = 0; run < this->kNumRandomRuns; ++run) {
      const std::vector<nimble::Vector<E>> dataPatterns =
          this->util_->template makeDataPatterns<E>(
              rng, this->kMaxRows, this->buffer_.get());
      for (const auto& data : dataPatterns) {
        const nimble::Vector<bool> nulls =
            this->makeRandomNulls(rng, data.size(), nullPattern);
        const nimble::Vector<E> spreadData =
            spreadNullsIntoData<E>(*this->pool_, data, nulls);

        auto encoding = nimble::test::Encoder<nimble::NullableEncoding<E>>::
            createNullableEncoding(*this->buffer_, data, nulls);
        ASSERT_TRUE(encoding->isNullable());
        const uint32_t rowCount = encoding->rowCount();
        nimble::Vector<E> buffer(this->pool_.get(), rowCount);
        nimble::Vector<char> bitmap(this->pool_.get(), rowCount);

        auto nonNullCount = encoding->materializeNullable(
            rowCount, buffer.data(), [&]() { return bitmap.data(); });
        EXPECT_EQ(
            std::accumulate(nulls.data(), nulls.data() + nulls.size(), 0),
            nonNullCount);

        for (int i = 0; i < rowCount; ++i) {
          checkOutput(
              i,
              nulls.data(),
              spreadData.data(),
              bitmap.data(),
              buffer.data(),
              nonNullCount != rowCount);
        }

        encoding->reset();
        const int firstBlock = rowCount / 2;
        nonNullCount = encoding->materializeNullable(
            firstBlock, buffer.data(), [&]() { return bitmap.data(); });
        EXPECT_EQ(
            std::accumulate(nulls.data(), nulls.data() + firstBlock, 0),
            nonNullCount);

        for (int i = 0; i < firstBlock; ++i) {
          checkOutput(
              i,
              nulls.data(),
              spreadData.data(),
              bitmap.data(),
              buffer.data(),
              nonNullCount != firstBlock);
        }
        const int secondBlock = rowCount - firstBlock;
        nonNullCount = encoding->materializeNullable(
            secondBlock, buffer.data(), [&]() { return bitmap.data(); });
        EXPECT_EQ(
            std::accumulate(
                nulls.data() + firstBlock, nulls.data() + rowCount, 0),
            nonNullCount);

        for (int i = 0; i < secondBlock; ++i) {
          checkOutput(
              i,
              nulls.data() + firstBlock,
              spreadData.data() + firstBlock,
              bitmap.data(),
              buffer.data(),
              nonNullCount != secondBlock);
        }

        encoding->reset();
        for (int i = 0; i < rowCount; ++i) {
          nonNullCount = encoding->materializeNullable(
              1, buffer.data(), [&]() { return bitmap.data(); });
          checkOutput(
              0,
              nulls.data() + i,
              spreadData.data() + i,
              bitmap.data(),
              buffer.data(),
              nonNullCount == 0);
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
          nonNullCount = encoding->materializeNullable(
              len, buffer.data(), [&]() { return bitmap.data(); });
          EXPECT_EQ(
              std::accumulate(
                  nulls.data() + start, nulls.data() + start + len, 0),
              nonNullCount);
          for (int j = 0; j < len; ++j) {
            checkOutput(
                j,
                nulls.data() + start,
                spreadData.data() + start,
                bitmap.data(),
                buffer.data(),
                nonNullCount != len);
          }
        }

        const uint32_t offset = folly::Random::rand32(rng) % data.size();
        const uint32_t length =
            1 + folly::Random::rand32(rng) % (data.size() - offset);
        encoding->reset();
        encoding->skip(offset);
        nonNullCount = encoding->materializeNullable(
            length, buffer.data(), [&]() { return bitmap.data(); });
        EXPECT_EQ(
            std::accumulate(
                nulls.data() + offset, nulls.data() + offset + length, 0),
            nonNullCount);
        for (uint32_t i = 0; i < length; ++i) {
          checkOutput(
              i,
              nulls.data() + offset,
              spreadData.data() + offset,
              bitmap.data(),
              buffer.data(),
              nonNullCount != length);
        }
      }
    }
  }
}
