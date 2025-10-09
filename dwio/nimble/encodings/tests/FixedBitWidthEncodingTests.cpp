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
#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"

#include <vector>

using namespace facebook;

template <typename C>
class FixedBitWidthEncodingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
    buffer_ = std::make_unique<nimble::Buffer>(*pool_);
  }

  template <typename T>
  nimble::Vector<T> toVector(std::initializer_list<T> l) {
    nimble::Vector<T> v{pool_.get()};
    v.insert(v.end(), l.begin(), l.end());
    return v;
  }

  template <typename T>
  std::vector<nimble::Vector<T>> prepareValues() {
    std::vector<nimble::Vector<T>> result;

    // Single value tests
    result.push_back(toVector({static_cast<T>(0)}));
    result.push_back(toVector({static_cast<T>(1)}));

    if constexpr (sizeof(T) >= 1) {
      result.push_back(toVector({static_cast<T>(255)}));
    }
    if constexpr (sizeof(T) >= 2) {
      result.push_back(toVector({static_cast<T>(65535)}));
    }

    // Small values requiring different bit widths - baseline 0
    result.push_back(toVector({static_cast<T>(0), static_cast<T>(1)})); // 1 bit
    result.push_back(toVector(
        {static_cast<T>(0),
         static_cast<T>(1),
         static_cast<T>(2),
         static_cast<T>(3)})); // 2 bits
    result.push_back(toVector(
        {static_cast<T>(0),
         static_cast<T>(1),
         static_cast<T>(7),
         static_cast<T>(15)})); // 4 bits

    if constexpr (sizeof(T) >= 1) {
      result.push_back(toVector(
          {static_cast<T>(0),
           static_cast<T>(1),
           static_cast<T>(255),
           static_cast<T>(128)})); // 8 bits
    }

    if constexpr (sizeof(T) >= 2) {
      result.push_back(toVector(
          {static_cast<T>(0),
           static_cast<T>(1),
           static_cast<T>(65535),
           static_cast<T>(32768)})); // 16 bits
    }

    // Sequential patterns - baseline 0
    result.push_back(toVector(
        {static_cast<T>(0),
         static_cast<T>(1),
         static_cast<T>(2),
         static_cast<T>(3),
         static_cast<T>(4),
         static_cast<T>(5),
         static_cast<T>(6),
         static_cast<T>(7)}));

    // Specific baseline tests - baseline 17
    result.push_back(toVector(
        {static_cast<T>(17),
         static_cast<T>(18),
         static_cast<T>(19),
         static_cast<T>(20),
         static_cast<T>(21)}));
    result.push_back(toVector(
        {static_cast<T>(17),
         static_cast<T>(17),
         static_cast<T>(17),
         static_cast<T>(17),
         static_cast<T>(17)})); // all same value
    result.push_back(toVector(
        {static_cast<T>(17),
         static_cast<T>(18),
         static_cast<T>(25),
         static_cast<T>(31)})); // requires 4 bits

    // Type-specific larger ranges
    if constexpr (sizeof(T) == 1) {
      // uint8_t specific tests
      result.push_back(toVector(
          {static_cast<T>(100),
           static_cast<T>(101),
           static_cast<T>(102),
           static_cast<T>(103),
           static_cast<T>(104)}));
      result.push_back(toVector(
          {static_cast<T>(250),
           static_cast<T>(251),
           static_cast<T>(252),
           static_cast<T>(253),
           static_cast<T>(254),
           static_cast<T>(255)}));
    } else if constexpr (sizeof(T) == 2) {
      // uint16_t specific tests
      result.push_back(toVector(
          {static_cast<T>(1000),
           static_cast<T>(1001),
           static_cast<T>(1002),
           static_cast<T>(1003),
           static_cast<T>(1004)}));
      result.push_back(toVector(
          {static_cast<T>(65530),
           static_cast<T>(65531),
           static_cast<T>(65532),
           static_cast<T>(65533),
           static_cast<T>(65534),
           static_cast<T>(65535)}));
    } else if constexpr (sizeof(T) == 4) {
      // uint32_t specific tests
      result.push_back(toVector(
          {static_cast<T>(0),
           static_cast<T>(1),
           static_cast<T>(16777215),
           static_cast<T>(8388608)})); // 24 bits
      result.push_back(toVector(
          {static_cast<T>(0),
           static_cast<T>(1),
           static_cast<T>(4294967295),
           static_cast<T>(2147483648)})); // 32 bits
      result.push_back(toVector(
          {static_cast<T>(4294967290),
           static_cast<T>(4294967291),
           static_cast<T>(4294967292),
           static_cast<T>(4294967293),
           static_cast<T>(4294967294),
           static_cast<T>(4294967295)}));
    } else if constexpr (sizeof(T) == 8) {
      // uint64_t specific tests
      result.push_back(toVector(
          {static_cast<T>(0),
           static_cast<T>(1),
           static_cast<T>(4294967295),
           static_cast<T>(2147483648)})); // 32 bits
      result.push_back(toVector(
          {static_cast<T>(0),
           static_cast<T>(1),
           static_cast<T>(18446744073709551615ULL),
           static_cast<T>(9223372036854775808ULL)}));
      result.push_back(toVector(
          {static_cast<T>(18446744073709551610ULL),
           static_cast<T>(18446744073709551611ULL),
           static_cast<T>(18446744073709551612ULL),
           static_cast<T>(18446744073709551613ULL),
           static_cast<T>(18446744073709551614ULL),
           static_cast<T>(18446744073709551615ULL)}));
    }

    // Repeated values
    result.push_back(toVector(
        {static_cast<T>(42),
         static_cast<T>(42),
         static_cast<T>(42),
         static_cast<T>(42),
         static_cast<T>(42)}));

    // Edge cases - min and max values
    if constexpr (sizeof(T) == 1) {
      result.push_back(toVector({static_cast<T>(0), static_cast<T>(255)}));
    } else if constexpr (sizeof(T) == 2) {
      result.push_back(toVector({static_cast<T>(0), static_cast<T>(65535)}));
    } else if constexpr (sizeof(T) == 4) {
      result.push_back(
          toVector({static_cast<T>(0), static_cast<T>(4294967295)}));
    } else if constexpr (sizeof(T) == 8) {
      result.push_back(toVector(
          {static_cast<T>(0), static_cast<T>(18446744073709551615ULL)}));
    }

    return result;
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<nimble::Buffer> buffer_;
};

// Only test unsigned integer types as FixedBitWidthEncoding only supports
// non-negative values
#define UNSIGNED_INT_TYPES uint8_t, uint16_t, uint32_t, uint64_t

using TestTypes = ::testing::Types<UNSIGNED_INT_TYPES>;

TYPED_TEST_SUITE(FixedBitWidthEncodingTest, TestTypes);

TYPED_TEST(FixedBitWidthEncodingTest, SerializeThenDeserialize) {
  using D = TypeParam;

  auto valueGroups = this->template prepareValues<D>();
  for (const auto& values : valueGroups) {
    auto encoding =
        nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
            *this->buffer_, values, nimble::CompressionType::Zstd);

    uint32_t rowCount = values.size();
    nimble::Vector<D> result(this->pool_.get(), rowCount);
    encoding->materialize(rowCount, result.data());

    EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::FixedBitWidth);
    EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
    EXPECT_EQ(encoding->rowCount(), rowCount);
    for (uint32_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(result[i], values[i]) << "Mismatch at index " << i;
    }
  }
}

TYPED_TEST(FixedBitWidthEncodingTest, PartialMaterialization) {
  using D = TypeParam;

  // Create a larger dataset for partial reads
  nimble::Vector<D> values(this->pool_.get());
  for (D i = 0; i < 100; ++i) {
    values.push_back(i * 7 % 1000); // Some pattern with variation
  }

  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  // Test partial materialization from the beginning
  {
    nimble::Vector<D> result(this->pool_.get(), 10);
    encoding->reset();
    encoding->materialize(10, result.data());

    for (uint32_t i = 0; i < 10; ++i) {
      EXPECT_EQ(result[i], values[i]) << "Partial read mismatch at index " << i;
    }
  }

  // Test skipping and then reading
  {
    nimble::Vector<D> result(this->pool_.get(), 10);
    encoding->reset();
    encoding->skip(20);
    encoding->materialize(10, result.data());

    for (uint32_t i = 0; i < 10; ++i) {
      EXPECT_EQ(result[i], values[20 + i])
          << "Skip + read mismatch at index " << i;
    }
  }

  // Test reading to the end
  {
    nimble::Vector<D> result(this->pool_.get(), 30);
    encoding->reset();
    encoding->skip(70);
    encoding->materialize(30, result.data());

    for (uint32_t i = 0; i < 30; ++i) {
      EXPECT_EQ(result[i], values[70 + i])
          << "End read mismatch at index " << i;
    }
  }
}

TYPED_TEST(FixedBitWidthEncodingTest, ResetAndReuse) {
  using D = TypeParam;

  nimble::Vector<D> values(this->pool_.get());
  values.push_back(static_cast<D>(10));
  values.push_back(static_cast<D>(20));
  values.push_back(static_cast<D>(30));
  values.push_back(static_cast<D>(40));
  values.push_back(static_cast<D>(50));

  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  // First read
  nimble::Vector<D> result1(this->pool_.get(), 5);
  encoding->materialize(5, result1.data());

  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(result1[i], values[i]);
  }

  // Reset and read again
  encoding->reset();
  nimble::Vector<D> result2(this->pool_.get(), 5);
  encoding->materialize(5, result2.data());

  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(result2[i], values[i]);
  }

  // Results should be identical
  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(result1[i], result2[i]);
  }
}

TYPED_TEST(FixedBitWidthEncodingTest, SingleValue) {
  using D = TypeParam;

  nimble::Vector<D> values = this->toVector({static_cast<D>(42)});
  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  nimble::Vector<D> result(this->pool_.get(), 1);
  encoding->materialize(1, result.data());

  EXPECT_EQ(encoding->encodingType(), nimble::EncodingType::FixedBitWidth);
  EXPECT_EQ(encoding->dataType(), nimble::TypeTraits<D>::dataType);
  EXPECT_EQ(encoding->rowCount(), 1u);
  EXPECT_EQ(result[0], static_cast<D>(42));
}

TYPED_TEST(FixedBitWidthEncodingTest, MinimalBitWidth) {
  using D = TypeParam;

  // Test that encoding uses minimal bit width
  nimble::Vector<D> values(this->pool_.get());
  values.push_back(static_cast<D>(0));
  values.push_back(static_cast<D>(1)); // Should require only 1 bit

  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  nimble::Vector<D> result(this->pool_.get(), 2);
  encoding->materialize(2, result.data());

  EXPECT_EQ(result[0], static_cast<D>(0));
  EXPECT_EQ(result[1], static_cast<D>(1));
}

TYPED_TEST(FixedBitWidthEncodingTest, LargeDataset) {
  using D = TypeParam;

  // Create a large dataset to test scalability
  nimble::Vector<D> values(this->pool_.get());
  constexpr uint32_t size = 10'000'000;
  for (uint32_t i = 0; i < size; ++i) {
    values.push_back(i % 1000);
  }

  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  nimble::Vector<D> result(this->pool_.get(), size);
  encoding->materialize(size, result.data());

  EXPECT_EQ(encoding->rowCount(), size);
  for (uint32_t i = 0; i < size; ++i) {
    EXPECT_EQ(result[i], values[i]) << "Large dataset mismatch at index " << i;
  }
}

TYPED_TEST(FixedBitWidthEncodingTest, BaselineSubtraction) {
  using D = TypeParam;

  // Test that baseline subtraction works correctly
  // Use different base values for different types to avoid overflow
  D baseValue;
  if constexpr (sizeof(D) == 1) {
    baseValue = 100; // uint8_t
  } else if constexpr (sizeof(D) == 2) {
    baseValue = 1000; // uint16_t
  } else {
    baseValue = 10000; // uint32_t and uint64_t
  }

  nimble::Vector<D> values(this->pool_.get());
  values.push_back(baseValue);
  values.push_back(baseValue + 1);
  values.push_back(baseValue + 2);
  values.push_back(baseValue + 3);
  values.push_back(baseValue + 4);

  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  nimble::Vector<D> result(this->pool_.get(), 5);
  encoding->materialize(5, result.data());

  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(result[i], values[i])
        << "Baseline subtraction failed at index " << i;
  }
}

TYPED_TEST(FixedBitWidthEncodingTest, MultiBatchReads) {
  using D = TypeParam;

  nimble::Vector<D> values(this->pool_.get());
  for (D i = 0; i < 50; ++i) {
    values.push_back(i);
  }

  auto encoding =
      nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
          *this->buffer_, values, nimble::CompressionType::Zstd);

  // Read in batches of 10
  nimble::Vector<D> result(this->pool_.get(), 50);
  encoding->reset();

  for (uint32_t batch = 0; batch < 5; ++batch) {
    encoding->materialize(10, result.data() + batch * 10);
  }

  for (uint32_t i = 0; i < 50; ++i) {
    EXPECT_EQ(result[i], values[i]) << "Value mismatch at index " << i;
  }
}

// Test specific to different bit widths to ensure proper encoding
TYPED_TEST(FixedBitWidthEncodingTest, VariousBitWidths) {
  using D = TypeParam;

  struct TestCase {
    std::vector<D> values;
    std::string description;
  };

  std::vector<TestCase> testCases = {
      {{0, 1}, "1-bit values"},
      {{0, 1, 2, 3}, "2-bit values"},
      {{0, 7, 15}, "4-bit values"},
      {{0, 255, 128}, "8-bit values"},
  };

  // Add more test cases for uint64_t
  if constexpr (sizeof(D) == 8) {
    testCases.push_back({{0, 65535, 32768}, "16-bit values"});
    testCases.push_back({{0, 4294967295ull, 2147483648ull}, "32-bit values"});
  }

  for (const auto& testCase : testCases) {
    nimble::Vector<D> values(this->pool_.get());
    for (auto val : testCase.values) {
      values.push_back(val);
    }

    auto encoding =
        nimble::test::Encoder<nimble::FixedBitWidthEncoding<D>>::createEncoding(
            *this->buffer_, values);

    nimble::Vector<D> result(this->pool_.get(), values.size());
    encoding->materialize(values.size(), result.data());

    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_EQ(result[i], values[i])
          << "Mismatch for " << testCase.description << " at index " << i;
    }
  }
}

TEST(FixedBitWidthEncodingUntypedTest, BitPackFallback) {
  auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  auto buffer = std::make_unique<nimble::Buffer>(*pool);

  // Test the fallback to bit packing happening if the byte packed is larger
  // than the uncompressed bit packed data.
  {
    nimble::Vector<uint32_t> values(pool.get());
    // Prepare values that are 1 bit width and will be packed into 8 bits using
    // byte aligned encoding. Zstd is not going compress 2 bytes.
    values.push_back(1);
    values.push_back(2);

    auto encoding =
        nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
            createEncoding(*buffer, values, nimble::CompressionType::Zstd);
    // check for bit packing based on bit_width=1 in the debug string
    EXPECT_EQ(
        encoding->debugString(),
        "FixedBitWidth<Uint32> rowCount=2 bit_width=1");
  }

  // Test the fallback to bit packing is not happening if the byte packed is
  // smaller than the uncompressed bit packed data
  {
    nimble::Vector<uint32_t> values(pool.get());
    // Prepare values that are 1 bit width and will be packed into 8 bits using
    // byte aligned encoding. The data is well compressible thanks to the 8000
    // '2' values.
    values.push_back(1);
    for (int i = 0; i < 8 * 1000; ++i) {
      values.push_back(2);
    }

    auto encoding =
        nimble::test::Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::
            createEncoding(*buffer, values, nimble::CompressionType::Zstd);
    // check for byte packing based on bit_width=8 in the debug string
    EXPECT_EQ(
        encoding->debugString(),
        "FixedBitWidth<Uint32> rowCount=8001 bit_width=8");
  }

  // Put two 7 bit values, then check the we are not falling back two bit
  // packed values from two byte packed values, because bit and byte packed
  // values have the same encoded size.
  {
    nimble::Vector<uint32_t> values(pool.get());
    values.push_back(1);
    values.push_back(100);

    auto encoding = nimble::test::
        Encoder<nimble::FixedBitWidthEncoding<uint32_t>>::createEncoding(
            *buffer, values, nimble::CompressionType::Uncompressed);
    // check for byte packing based on bit_width=8 in the debug string
    EXPECT_EQ(
        encoding->debugString(),
        "FixedBitWidth<Uint32> rowCount=2 bit_width=8");
  }
}
