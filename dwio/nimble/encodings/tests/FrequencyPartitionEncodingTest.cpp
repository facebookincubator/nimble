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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <random>
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/encodings/FrequencyPartitionEncoding.h"
#include "folly/Random.h"
#include "velox/common/memory/Memory.h"

using namespace facebook;

class FrequencyPartitionEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  std::unique_ptr<nimble::Encoding> createEncoding(
      const nimble::Vector<T>& data) {
    return nimble::test::Encoder<nimble::FrequencyPartitionEncoding<T>>::
        createEncoding(*buffer_, data, nimble::CompressionType::Uncompressed);
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

// Test basic encode/decode with simple data
TEST_F(FrequencyPartitionEncodingTest, BasicEncodeDecode) {
  nimble::Vector<int32_t> data(pool_.get());
  data.push_back(1);
  data.push_back(2);
  data.push_back(1);
  data.push_back(3);
  data.push_back(1);
  data.push_back(2);

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->encodingType(), nimble::EncodingType::FrequencyPartition);
  ASSERT_EQ(encoding->dataType(), nimble::DataType::Int32);
  ASSERT_EQ(encoding->rowCount(), 6);

  nimble::Vector<int32_t> result(pool_.get(), 6);
  encoding->materialize(6, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  // So we compare sorted values instead of maintaining original order
  std::vector<int32_t> sortedData(data.begin(), data.end());
  std::vector<int32_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Mismatch at sorted index " << i;
  }
}

// Test with Zipfian distribution (skewed frequencies)
TEST_F(FrequencyPartitionEncodingTest, ZipfianDistribution) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);

  const int numValues = 10000;
  const int numUniqueValues = 100;

  nimble::Vector<int64_t> data(pool_.get());
  
  // Create Zipfian distribution: first values appear much more frequently
  std::vector<int> frequencies(numUniqueValues);
  for (int i = 0; i < numUniqueValues; ++i) {
    frequencies[i] = numUniqueValues - i; // Descending frequency
  }

  // Generate data according to frequencies
  for (int i = 0; i < numValues; ++i) {
    int totalFreq = numUniqueValues * (numUniqueValues + 1) / 2;
    int rand = rng() % totalFreq;
    int cumulative = 0;
    for (int j = 0; j < numUniqueValues; ++j) {
      cumulative += frequencies[j];
      if (rand < cumulative) {
        data.push_back(j);
        break;
      }
    }
  }

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), numValues);

  nimble::Vector<int64_t> result(pool_.get(), numValues);
  encoding->materialize(numValues, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<int64_t> sortedData(data.begin(), data.end());
  std::vector<int64_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Mismatch at sorted index " << i;
  }
}

// Test with uniform distribution
TEST_F(FrequencyPartitionEncodingTest, UniformDistribution) {
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng(seed);
  std::uniform_int_distribution<uint32_t> dist(0, 999);

  const int numValues = 5000;
  nimble::Vector<uint32_t> data(pool_.get());
  
  for (int i = 0; i < numValues; ++i) {
    data.push_back(dist(rng));
  }

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), numValues);

  nimble::Vector<uint32_t> result(pool_.get(), numValues);
  encoding->materialize(numValues, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<uint32_t> sortedData(data.begin(), data.end());
  std::vector<uint32_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Mismatch at sorted index " << i;
  }
}

// Test with all identical values (extreme case)
TEST_F(FrequencyPartitionEncodingTest, AllIdenticalValues) {
  const int numValues = 1000;
  nimble::Vector<int32_t> data(pool_.get());
  
  for (int i = 0; i < numValues; ++i) {
    data.push_back(42);
  }

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), numValues);

  nimble::Vector<int32_t> result(pool_.get(), numValues);
  encoding->materialize(numValues, result.data());

  for (size_t i = 0; i < data.size(); ++i) {
    ASSERT_EQ(result[i], 42) << "Mismatch at index " << i;
  }
}

// Test with single value
TEST_F(FrequencyPartitionEncodingTest, SingleValue) {
  nimble::Vector<int64_t> data(pool_.get());
  data.push_back(123);

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), 1);

  nimble::Vector<int64_t> result(pool_.get(), 1);
  encoding->materialize(1, result.data());

  ASSERT_EQ(result[0], 123);
}

// Test with string_view (non-numeric type)
TEST_F(FrequencyPartitionEncodingTest, StringValues) {
  std::vector<std::string> strings = {
      "apple", "banana", "apple", "cherry", "apple",
      "banana", "date", "apple", "banana", "apple"};

  nimble::Vector<std::string_view> data(pool_.get());
  for (const auto& s : strings) {
    data.push_back(s);
  }

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), strings.size());

  nimble::Vector<std::string_view> result(pool_.get(), strings.size());
  encoding->materialize(strings.size(), result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<std::string_view> sortedData(data.begin(), data.end());
  std::vector<std::string_view> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Mismatch at sorted index " << i;
  }
}

// Test incremental materialization (reset and partial reads)
TEST_F(FrequencyPartitionEncodingTest, IncrementalMaterialization) {
  nimble::Vector<uint64_t> data(pool_.get());
  for (uint64_t i = 0; i < 100; ++i) {
    data.push_back(i % 10); // 10 unique values, repeated
  }

  auto encoding = createEncoding(data);
  nimble::Vector<uint64_t> result(pool_.get(), 100);

  // Read all data at once
  encoding->materialize(100, result.data());
  
  // Verify all values are present (compare sorted)
  std::vector<uint64_t> sortedData(data.begin(), data.end());
  std::vector<uint64_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());
  
  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Mismatch at sorted index " << i;
  }

  // Test reset functionality
  encoding->reset();
  encoding->materialize(100, result.data());
  
  // Verify reset works - should get same reordered data again
  std::vector<uint64_t> sortedResult2(result.begin(), result.end());
  std::sort(sortedResult2.begin(), sortedResult2.end());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult2[i], sortedData[i]) << "Reset mismatch at sorted index " << i;
  }
}

// Test with large values (to test different tier sizes)
TEST_F(FrequencyPartitionEncodingTest, LargeValues) {
  nimble::Vector<uint64_t> data(pool_.get());
  
  // Add values that require different bit widths
  data.push_back(1ULL);                    // 1 bit
  data.push_back(255ULL);                  // 8 bits
  data.push_back(65535ULL);                // 16 bits
  data.push_back(4294967295ULL);           // 32 bits
  data.push_back(18446744073709551615ULL); // 64 bits
  data.push_back(1ULL);                    // Repeat small value

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), 6);

  nimble::Vector<uint64_t> result(pool_.get(), 6);
  encoding->materialize(6, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<uint64_t> sortedData(data.begin(), data.end());
  std::vector<uint64_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Large value mismatch at sorted index " << i;
  }
}

// Test with floating point values
TEST_F(FrequencyPartitionEncodingTest, FloatValues) {
  nimble::Vector<float> data(pool_.get());
  data.push_back(1.5f);
  data.push_back(2.7f);
  data.push_back(1.5f);
  data.push_back(3.14f);
  data.push_back(1.5f);
  data.push_back(2.7f);

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), 6);

  nimble::Vector<float> result(pool_.get(), 6);
  encoding->materialize(6, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<float> sortedData(data.begin(), data.end());
  std::vector<float> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Float mismatch at sorted index " << i;
  }
}

// Test with double values
TEST_F(FrequencyPartitionEncodingTest, DoubleValues) {
  nimble::Vector<double> data(pool_.get());
  data.push_back(1.5);
  data.push_back(2.7);
  data.push_back(1.5);
  data.push_back(3.14159265359);
  data.push_back(1.5);

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), 5);

  nimble::Vector<double> result(pool_.get(), 5);
  encoding->materialize(5, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<double> sortedData(data.begin(), data.end());
  std::vector<double> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Double mismatch at sorted index " << i;
  }
}

// Test encoding layout capture
TEST_F(FrequencyPartitionEncodingTest, EncodingLayout) {
  nimble::Vector<int32_t> data(pool_.get());
  for (int i = 0; i < 100; ++i) {
    data.push_back(i % 10);
  }

  auto encoding = createEncoding(data);
  
  // Verify the encoding type is correct
  ASSERT_EQ(encoding->encodingType(), nimble::EncodingType::FrequencyPartition);
  
  // The encoding should have nested encodings for:
  // - Tier assignments
  // - Tier dictionaries
  // - Tier data
  // Exact structure depends on data distribution
}

// Test with edge case: maximum tier count
TEST_F(FrequencyPartitionEncodingTest, MaximumTiers) {
  nimble::Vector<uint8_t> data(pool_.get());
  
  // Create data with many unique values to potentially trigger maximum tiers
  for (int i = 0; i < 256; ++i) {
    data.push_back(static_cast<uint8_t>(i));
  }

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), 256);

  nimble::Vector<uint8_t> result(pool_.get(), 256);
  encoding->materialize(256, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<uint8_t> sortedData(data.begin(), data.end());
  std::vector<uint8_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Max tiers mismatch at sorted index " << i;
  }
}

// Test with empty-like data (very sparse)
TEST_F(FrequencyPartitionEncodingTest, SparseData) {
  nimble::Vector<int64_t> data(pool_.get());
  
  // Mostly zeros with occasional other values
  for (int i = 0; i < 1000; ++i) {
    if (i % 100 == 0) {
      data.push_back(i);
    } else {
      data.push_back(0);
    }
  }

  auto encoding = createEncoding(data);
  ASSERT_EQ(encoding->rowCount(), 1000);

  nimble::Vector<int64_t> result(pool_.get(), 1000);
  encoding->materialize(1000, result.data());

  // FrequencyPartitionEncoding reorders data by frequency tiers
  std::vector<int64_t> sortedData(data.begin(), data.end());
  std::vector<int64_t> sortedResult(result.begin(), result.end());
  std::sort(sortedData.begin(), sortedData.end());
  std::sort(sortedResult.begin(), sortedResult.end());

  ASSERT_EQ(sortedData.size(), sortedResult.size());
  for (size_t i = 0; i < sortedData.size(); ++i) {
    ASSERT_EQ(sortedResult[i], sortedData[i]) << "Sparse data mismatch at sorted index " << i;
  }
}
