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
#include <gtest/gtest.h>

#include "dwio/nimble/velox/RawSizeUtils.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook;

constexpr velox::vector_size_t VECTOR_SIZE = 100;

std::function<bool(velox::vector_size_t)> randomNulls(int32_t n) {
  return [n](velox::vector_size_t /*index*/) {
    return folly::Random::rand32() % n == 0;
  };
}

std::function<bool(velox::vector_size_t)> noNulls() {
  return [](velox::vector_size_t /*index*/) { return false; };
}

std::string generateRandomString() {
  auto length = folly::Random::rand32() % VECTOR_SIZE + 1;
  return std::string(length, 'A');
}

// Fixed width types
template <typename T>
T getValue(velox::vector_size_t i) {
  return i;
}

// Fixed width types
template <typename T>
uint64_t getSize(T /*value*/) {
  return sizeof(T);
}

// Specialization for StringView type
template <>
velox::StringView getValue<velox::StringView>(velox::vector_size_t /*i*/) {
  static std::shared_ptr<std::string> str =
      std::make_shared<std::string>(generateRandomString());
  return velox::StringView(str->data(), str->size());
}

// Specialization for StringView type
template <>
uint64_t getSize<velox::StringView>(velox::StringView value) {
  return value.size();
}

template <>
velox::Timestamp getValue<velox::Timestamp>(velox::vector_size_t /*i*/) {
  int64_t seconds = folly::Random::rand32() % 1'000'000'000;
  uint64_t nanos = folly::Random::rand32() % 1'000'000'000;

  return velox::Timestamp(seconds, nanos);
}

template <>
uint64_t getSize<velox::Timestamp>(velox::Timestamp /*value*/) {
  return sizeof(int64_t) + sizeof(uint64_t);
}

class RawSizeBaseTestFixture : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::initialize({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
    vectorMaker_ =
        std::make_unique<velox::test::VectorMaker>(this->pool_.get());
    ranges_.clear();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  velox::common::Ranges ranges_;
  std::unique_ptr<velox::test::VectorMaker> vectorMaker_;
};

class RawSizeTestFixture : public RawSizeBaseTestFixture {
 protected:
  void reset() {
    ranges_.clear();
  }

  template <typename T>
  void testFlat(const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<std::optional<T>> vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    for (velox::vector_size_t i = 1; i <= VECTOR_SIZE; ++i) {
      if (isNullAt(i)) {
        expectedRawSize += nimble::NULL_SIZE;
        vec.emplace_back(std::nullopt);
      } else {
        auto value = getValue<T>(i);
        expectedRawSize += getSize<T>(value);
        vec.emplace_back(value);
      }
    }
    auto flatVector = vectorMaker_->flatVectorNullable<T>(vec);
    ranges_.add(0, flatVector->size());
    auto rawSize = nimble::getRawSizeFromVector(flatVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename T>
  void testConstant(const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<std::optional<T>> vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    if (isNullAt(1)) {
      vec.assign(VECTOR_SIZE, std::nullopt);
      expectedRawSize += nimble::NULL_SIZE * VECTOR_SIZE;
    } else {
      auto valueAt = getValue<T>(1);
      expectedRawSize += getSize<T>(valueAt) * VECTOR_SIZE;
      vec.assign(VECTOR_SIZE, valueAt);
    }
    auto flatVector = vectorMaker_->constantVector<T>(vec);
    ranges_.add(0, flatVector->size());
    auto rawSize = nimble::getRawSizeFromVector(flatVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename T>
  void testDictionary(
      const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<std::optional<T>> vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    for (velox::vector_size_t i = 1; i <= VECTOR_SIZE; ++i) {
      if (isNullAt(i)) {
        vec.emplace_back(std::nullopt);
      } else {
        auto value = getValue<T>(i);
        vec.emplace_back(value);
      }
    }
    auto flatVector = vectorMaker_->flatVectorNullable<T>(vec);
    auto indices = randomIndices(VECTOR_SIZE);
    const velox::vector_size_t* data = indices->as<velox::vector_size_t>();
    for (auto i = 0; i < VECTOR_SIZE; ++i) {
      if (vec[data[i]] == std::nullopt) {
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        expectedRawSize += getSize<T>(vec[data[i]].value());
      }
    }
    auto wrappedVector = velox::BaseVector::wrapInDictionary(
        velox::BufferPtr(nullptr), indices, VECTOR_SIZE, flatVector);
    ranges_.add(0, wrappedVector->size());
    auto rawSize = nimble::getRawSizeFromVector(wrappedVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename T>
  void testArray(const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<std::optional<std::vector<std::optional<T>>>> vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    for (velox::vector_size_t i = 1; i <= VECTOR_SIZE; ++i) {
      if (isNullAt(i)) {
        vec.emplace_back(std::nullopt);
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        std::vector<std::optional<T>> innerVec;
        velox::vector_size_t innerSize =
            folly::Random::rand32() % VECTOR_SIZE + 1;
        for (velox::vector_size_t j = 0; j < innerSize; ++j) {
          if (isNullAt(j)) {
            innerVec.emplace_back(std::nullopt);
            expectedRawSize += nimble::NULL_SIZE;
          } else {
            auto value = getValue<T>(j);
            expectedRawSize += getSize<T>(value);
            innerVec.emplace_back(value);
          }
        }
        vec.emplace_back(innerVec);
      }
    }
    auto arrayVector = vectorMaker_->arrayVectorNullable<T>(vec);
    ranges_.add(0, arrayVector->size());
    auto rawSize = nimble::getRawSizeFromVector(arrayVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename T>
  void testConstantArray(
      const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<std::optional<std::vector<std::optional<T>>>> vec;
    uint64_t expectedRawSize = 0;
    velox::vector_size_t innerSize = folly::Random::rand32() % VECTOR_SIZE + 1;
    std::vector<std::optional<T>> innerVec;
    for (velox::vector_size_t j = 0; j < innerSize; ++j) {
      if (isNullAt(j)) {
        innerVec.emplace_back(std::nullopt);
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        auto value = getValue<T>(j);
        expectedRawSize += getSize<T>(value);
        innerVec.emplace_back(value);
      }
    }
    expectedRawSize *= VECTOR_SIZE;
    vec.emplace_back(innerVec);
    auto arrayVector = vectorMaker_->arrayVectorNullable<T>(vec);
    auto constVector =
        velox::BaseVector::wrapInConstant(VECTOR_SIZE, 0, arrayVector);
    ranges_.add(0, constVector->size());
    auto rawSize = nimble::getRawSizeFromVector(constVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename T>
  void testDictionaryArray(
      const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<std::optional<std::vector<std::optional<T>>>> vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    std::unordered_map<velox::vector_size_t, uint64_t> indexToSize;
    for (velox::vector_size_t i = 1; i <= VECTOR_SIZE; ++i) {
      if (isNullAt(i)) {
        vec.emplace_back(std::nullopt);
      } else {
        std::vector<std::optional<T>> innerVec;
        uint64_t size = 0;
        velox::vector_size_t innerSize =
            folly::Random::rand32() % VECTOR_SIZE + 1;
        for (velox::vector_size_t j = 0; j < innerSize; ++j) {
          if (isNullAt(j)) {
            size += nimble::NULL_SIZE;
            innerVec.emplace_back(std::nullopt);
          } else {
            auto value = getValue<T>(j);
            size += getSize<T>(value);
            innerVec.emplace_back(value);
          }
        }
        indexToSize[i - 1] = size;
        vec.emplace_back(innerVec);
      }
    }
    auto arrayVector = vectorMaker_->arrayVectorNullable<T>(vec);
    auto indices = randomIndices(VECTOR_SIZE);
    const velox::vector_size_t* data = indices->as<velox::vector_size_t>();
    for (auto i = 0; i < VECTOR_SIZE; ++i) {
      if (vec[data[i]] == std::nullopt) {
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        expectedRawSize += indexToSize[data[i]];
      }
    }
    auto wrappedVector = velox::BaseVector::wrapInDictionary(
        velox::BufferPtr(nullptr), indices, VECTOR_SIZE, arrayVector);
    ranges_.add(0, wrappedVector->size());
    auto rawSize = nimble::getRawSizeFromVector(wrappedVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename TKey, typename TValue>
  void testMap(const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<
        std::optional<std::vector<std::pair<TKey, std::optional<TValue>>>>>
        vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    for (velox::vector_size_t i = 1; i <= VECTOR_SIZE; ++i) {
      if (isNullAt(i)) {
        vec.emplace_back(std::nullopt);
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        std::vector<std::pair<TKey, std::optional<TValue>>> innerVec;
        velox::vector_size_t innerSize =
            folly::Random::rand32() % VECTOR_SIZE + 1;
        for (velox::vector_size_t j = 0; j < innerSize; ++j) {
          TKey key = getValue<TKey>(j);
          if (isNullAt(j)) {
            innerVec.emplace_back(key, std::nullopt);
            expectedRawSize += nimble::NULL_SIZE;
          } else {
            auto value = getValue<TValue>(j);
            expectedRawSize += getSize<TValue>(value);
            innerVec.emplace_back(key, value);
          }
          expectedRawSize += getSize<TKey>(key);
        }
        vec.emplace_back(innerVec);
      }
    }
    auto mapVector = vectorMaker_->mapVector<TKey, TValue>(vec);
    ranges_.add(0, mapVector->size());
    auto rawSize = nimble::getRawSizeFromVector(mapVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename TKey, typename TValue>
  void testConstantMap(
      const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<
        std::optional<std::vector<std::pair<TKey, std::optional<TValue>>>>>
        vec;
    uint64_t expectedRawSize = 0;
    std::vector<std::pair<TKey, std::optional<TValue>>> innerVec;
    velox::vector_size_t innerSize = folly::Random::rand32() % VECTOR_SIZE + 1;
    for (velox::vector_size_t j = 0; j < innerSize; ++j) {
      TKey key = getValue<TKey>(j);
      if (isNullAt(j)) {
        innerVec.emplace_back(key, std::nullopt);
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        auto value = getValue<TValue>(j);
        expectedRawSize += getSize<TValue>(value);
        innerVec.emplace_back(key, value);
      }
      expectedRawSize += getSize<TKey>(key);
    }
    expectedRawSize *= VECTOR_SIZE;
    vec.emplace_back(innerVec);
    auto mapVector = vectorMaker_->mapVector<TKey, TValue>(vec);
    auto constVector =
        velox::BaseVector::wrapInConstant(VECTOR_SIZE, 0, mapVector);
    ranges_.add(0, constVector->size());
    auto rawSize = nimble::getRawSizeFromVector(constVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

  template <typename TKey, typename TValue>
  void testDictionaryMap(
      const std::function<bool(velox::vector_size_t)>& isNullAt) {
    reset();
    std::vector<
        std::optional<std::vector<std::pair<TKey, std::optional<TValue>>>>>
        vec;
    vec.reserve(VECTOR_SIZE);
    uint64_t expectedRawSize = 0;
    std::unordered_map<velox::vector_size_t, uint64_t> indexToSize;
    for (velox::vector_size_t i = 1; i <= VECTOR_SIZE; ++i) {
      if (isNullAt(i)) {
        vec.emplace_back(std::nullopt);
      } else {
        std::vector<std::pair<TKey, std::optional<TValue>>> innerVec;
        uint64_t size = 0;
        velox::vector_size_t innerSize =
            folly::Random::rand32() % VECTOR_SIZE + 1;
        for (velox::vector_size_t j = 0; j < innerSize; ++j) {
          TKey key = getValue<TKey>(j);
          if (isNullAt(j)) {
            size += nimble::NULL_SIZE;
            innerVec.emplace_back(key, std::nullopt);
          } else {
            auto value = getValue<TValue>(j);
            size += getSize<TValue>(value);
            innerVec.emplace_back(key, value);
          }
          size += getSize<TKey>(key);
        }
        indexToSize[i - 1] = size;
        vec.emplace_back(innerVec);
      }
    }
    auto mapVector = vectorMaker_->mapVector<TKey, TValue>(vec);
    auto indices = randomIndices(VECTOR_SIZE);
    const velox::vector_size_t* data = indices->as<velox::vector_size_t>();
    for (auto i = 0; i < VECTOR_SIZE; ++i) {
      if (vec[data[i]] == std::nullopt) {
        expectedRawSize += nimble::NULL_SIZE;
      } else {
        expectedRawSize += indexToSize[data[i]];
      }
    }
    auto wrappedVector = velox::BaseVector::wrapInDictionary(
        velox::BufferPtr(nullptr), indices, VECTOR_SIZE, mapVector);
    ranges_.add(0, wrappedVector->size());
    auto rawSize = nimble::getRawSizeFromVector(wrappedVector, ranges_);
    ASSERT_EQ(expectedRawSize, rawSize);
  }

 private:
  velox::BufferPtr randomIndices(velox::vector_size_t size) {
    velox::BufferPtr indices =
        velox::AlignedBuffer::allocate<velox::vector_size_t>(size, pool_.get());
    auto rawIndices = indices->asMutable<velox::vector_size_t>();
    for (int32_t i = 0; i < size; i++) {
      rawIndices[i] = folly::Random::rand32(size);
    }
    return indices;
  }
};

/*
 * The following tests are considered Fuzz tests. The data inside the vectors,
 * as well as the null count and positions, are randomized. The expected raw
 * size is calculated from this random data and is used to assert against the
 * raw size returned by the function under test.
 */
TEST_F(RawSizeTestFixture, Flat) {
  testFlat<bool>(noNulls());
  testFlat<int8_t>(noNulls());
  testFlat<int16_t>(noNulls());
  testFlat<int32_t>(noNulls());
  testFlat<int64_t>(noNulls());
  testFlat<float>(noNulls());
  testFlat<double>(noNulls());
  testFlat<velox::StringView>(noNulls());
  testFlat<velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, FlatSomeNull) {
  testFlat<bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<velox::StringView>(randomNulls(folly::Random::rand32() % 10 + 1));
  testFlat<velox::Timestamp>(randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, Constant) {
  testConstant<bool>(noNulls());
  testConstant<int8_t>(noNulls());
  testConstant<int16_t>(noNulls());
  testConstant<int32_t>(noNulls());
  testConstant<int64_t>(noNulls());
  testConstant<float>(noNulls());
  testConstant<double>(noNulls());
  testConstant<velox::StringView>(noNulls());
  testConstant<velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, ConstantSomeNull) {
  testConstant<bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstant<velox::Timestamp>(randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, Dictionary) {
  testDictionary<bool>(noNulls());
  testDictionary<int8_t>(noNulls());
  testDictionary<int16_t>(noNulls());
  testDictionary<int32_t>(noNulls());
  testDictionary<int64_t>(noNulls());
  testDictionary<float>(noNulls());
  testDictionary<double>(noNulls());
  testDictionary<velox::StringView>(noNulls());
  testDictionary<velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, DictionarySomeNull) {
  testDictionary<bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionary<velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, Array) {
  testArray<bool>(noNulls());
  testArray<int8_t>(noNulls());
  testArray<int16_t>(noNulls());
  testArray<int32_t>(noNulls());
  testArray<int64_t>(noNulls());
  testArray<float>(noNulls());
  testArray<double>(noNulls());
  testArray<velox::StringView>(noNulls());
  testArray<velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, ArraySomNull) {
  testArray<bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<velox::StringView>(randomNulls(folly::Random::rand32() % 10 + 1));
  testArray<velox::Timestamp>(randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, ConstantArray) {
  testConstantArray<bool>(noNulls());
  testConstantArray<int8_t>(noNulls());
  testConstantArray<int16_t>(noNulls());
  testConstantArray<int32_t>(noNulls());
  testConstantArray<int64_t>(noNulls());
  testConstantArray<float>(noNulls());
  testConstantArray<double>(noNulls());
  testConstantArray<velox::StringView>(noNulls());
  testConstantArray<velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, ConstantArraySomeNull) {
  testConstantArray<bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantArray<velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, DictionaryArray) {
  testDictionaryArray<bool>(noNulls());
  testDictionaryArray<int8_t>(noNulls());
  testDictionaryArray<int16_t>(noNulls());
  testDictionaryArray<int32_t>(noNulls());
  testDictionaryArray<int64_t>(noNulls());
  testDictionaryArray<float>(noNulls());
  testDictionaryArray<double>(noNulls());
  testDictionaryArray<velox::StringView>(noNulls());
  testDictionaryArray<velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, DictionaryArraySomeNull) {
  testDictionaryArray<bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryArray<velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, Map) {
  testMap<bool, bool>(noNulls());
  testMap<bool, int8_t>(noNulls());
  testMap<bool, int16_t>(noNulls());
  testMap<bool, int32_t>(noNulls());
  testMap<bool, int64_t>(noNulls());
  testMap<bool, float>(noNulls());
  testMap<bool, double>(noNulls());
  testMap<bool, velox::StringView>(noNulls());
  testMap<bool, velox::Timestamp>(noNulls());

  testMap<int8_t, bool>(noNulls());
  testMap<int8_t, int8_t>(noNulls());
  testMap<int8_t, int16_t>(noNulls());
  testMap<int8_t, int32_t>(noNulls());
  testMap<int8_t, int64_t>(noNulls());
  testMap<int8_t, float>(noNulls());
  testMap<int8_t, double>(noNulls());
  testMap<int8_t, velox::StringView>(noNulls());
  testMap<int8_t, velox::Timestamp>(noNulls());

  testMap<int16_t, bool>(noNulls());
  testMap<int16_t, int8_t>(noNulls());
  testMap<int16_t, int16_t>(noNulls());
  testMap<int16_t, int32_t>(noNulls());
  testMap<int16_t, int64_t>(noNulls());
  testMap<int16_t, float>(noNulls());
  testMap<int16_t, double>(noNulls());
  testMap<int16_t, velox::StringView>(noNulls());
  testMap<int16_t, velox::Timestamp>(noNulls());

  testMap<int32_t, bool>(noNulls());
  testMap<int32_t, int8_t>(noNulls());
  testMap<int32_t, int16_t>(noNulls());
  testMap<int32_t, int32_t>(noNulls());
  testMap<int32_t, int64_t>(noNulls());
  testMap<int32_t, float>(noNulls());
  testMap<int32_t, double>(noNulls());
  testMap<int32_t, velox::StringView>(noNulls());
  testMap<int32_t, velox::Timestamp>(noNulls());

  testMap<int64_t, bool>(noNulls());
  testMap<int64_t, int8_t>(noNulls());
  testMap<int64_t, int16_t>(noNulls());
  testMap<int64_t, int32_t>(noNulls());
  testMap<int64_t, int64_t>(noNulls());
  testMap<int64_t, float>(noNulls());
  testMap<int64_t, double>(noNulls());
  testMap<int64_t, velox::StringView>(noNulls());
  testMap<int64_t, velox::Timestamp>(noNulls());

  testMap<float, bool>(noNulls());
  testMap<float, int8_t>(noNulls());
  testMap<float, int16_t>(noNulls());
  testMap<float, int32_t>(noNulls());
  testMap<float, int64_t>(noNulls());
  testMap<float, float>(noNulls());
  testMap<float, double>(noNulls());
  testMap<float, velox::StringView>(noNulls());
  testMap<float, velox::Timestamp>(noNulls());

  testMap<double, bool>(noNulls());
  testMap<double, int8_t>(noNulls());
  testMap<double, int16_t>(noNulls());
  testMap<double, int32_t>(noNulls());
  testMap<double, int64_t>(noNulls());
  testMap<double, float>(noNulls());
  testMap<double, double>(noNulls());
  testMap<double, velox::StringView>(noNulls());
  testMap<double, velox::Timestamp>(noNulls());

  testMap<velox::StringView, bool>(noNulls());
  testMap<velox::StringView, int8_t>(noNulls());
  testMap<velox::StringView, int16_t>(noNulls());
  testMap<velox::StringView, int32_t>(noNulls());
  testMap<velox::StringView, int64_t>(noNulls());
  testMap<velox::StringView, float>(noNulls());
  testMap<velox::StringView, double>(noNulls());
  testMap<velox::StringView, velox::StringView>(noNulls());
  testMap<velox::StringView, velox::Timestamp>(noNulls());

  testMap<velox::Timestamp, bool>(noNulls());
  testMap<velox::Timestamp, int8_t>(noNulls());
  testMap<velox::Timestamp, int16_t>(noNulls());
  testMap<velox::Timestamp, int32_t>(noNulls());
  testMap<velox::Timestamp, int64_t>(noNulls());
  testMap<velox::Timestamp, float>(noNulls());
  testMap<velox::Timestamp, double>(noNulls());
  testMap<velox::Timestamp, velox::StringView>(noNulls());
  testMap<velox::Timestamp, velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, MapSomeNull) {
  testMap<bool, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<bool, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<int8_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int8_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<int16_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int16_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<int32_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int32_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<int64_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<int64_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<float, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<float, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<double, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<double, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<velox::StringView, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::StringView, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testMap<velox::Timestamp, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testMap<velox::Timestamp, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, ConstantMap) {
  testConstantMap<bool, bool>(noNulls());
  testConstantMap<bool, int8_t>(noNulls());
  testConstantMap<bool, int16_t>(noNulls());
  testConstantMap<bool, int32_t>(noNulls());
  testConstantMap<bool, int64_t>(noNulls());
  testConstantMap<bool, float>(noNulls());
  testConstantMap<bool, double>(noNulls());
  testConstantMap<bool, velox::StringView>(noNulls());
  testConstantMap<bool, velox::Timestamp>(noNulls());

  testConstantMap<int8_t, bool>(noNulls());
  testConstantMap<int8_t, int8_t>(noNulls());
  testConstantMap<int8_t, int16_t>(noNulls());
  testConstantMap<int8_t, int32_t>(noNulls());
  testConstantMap<int8_t, int64_t>(noNulls());
  testConstantMap<int8_t, float>(noNulls());
  testConstantMap<int8_t, double>(noNulls());
  testConstantMap<int8_t, velox::StringView>(noNulls());
  testConstantMap<int8_t, velox::Timestamp>(noNulls());

  testConstantMap<int16_t, bool>(noNulls());
  testConstantMap<int16_t, int8_t>(noNulls());
  testConstantMap<int16_t, int16_t>(noNulls());
  testConstantMap<int16_t, int32_t>(noNulls());
  testConstantMap<int16_t, int64_t>(noNulls());
  testConstantMap<int16_t, float>(noNulls());
  testConstantMap<int16_t, double>(noNulls());
  testConstantMap<int16_t, velox::StringView>(noNulls());
  testConstantMap<int16_t, velox::Timestamp>(noNulls());

  testConstantMap<int32_t, bool>(noNulls());
  testConstantMap<int32_t, int8_t>(noNulls());
  testConstantMap<int32_t, int16_t>(noNulls());
  testConstantMap<int32_t, int32_t>(noNulls());
  testConstantMap<int32_t, int64_t>(noNulls());
  testConstantMap<int32_t, float>(noNulls());
  testConstantMap<int32_t, double>(noNulls());
  testConstantMap<int32_t, velox::StringView>(noNulls());
  testConstantMap<int32_t, velox::Timestamp>(noNulls());

  testConstantMap<int64_t, bool>(noNulls());
  testConstantMap<int64_t, int8_t>(noNulls());
  testConstantMap<int64_t, int16_t>(noNulls());
  testConstantMap<int64_t, int32_t>(noNulls());
  testConstantMap<int64_t, int64_t>(noNulls());
  testConstantMap<int64_t, float>(noNulls());
  testConstantMap<int64_t, double>(noNulls());
  testConstantMap<int64_t, velox::StringView>(noNulls());
  testConstantMap<int64_t, velox::Timestamp>(noNulls());

  testConstantMap<float, bool>(noNulls());
  testConstantMap<float, int8_t>(noNulls());
  testConstantMap<float, int16_t>(noNulls());
  testConstantMap<float, int32_t>(noNulls());
  testConstantMap<float, int64_t>(noNulls());
  testConstantMap<float, float>(noNulls());
  testConstantMap<float, double>(noNulls());
  testConstantMap<float, velox::StringView>(noNulls());
  testConstantMap<float, velox::Timestamp>(noNulls());

  testConstantMap<double, bool>(noNulls());
  testConstantMap<double, int8_t>(noNulls());
  testConstantMap<double, int16_t>(noNulls());
  testConstantMap<double, int32_t>(noNulls());
  testConstantMap<double, int64_t>(noNulls());
  testConstantMap<double, float>(noNulls());
  testConstantMap<double, double>(noNulls());
  testConstantMap<double, velox::StringView>(noNulls());
  testConstantMap<double, velox::Timestamp>(noNulls());

  testConstantMap<velox::StringView, bool>(noNulls());
  testConstantMap<velox::StringView, int8_t>(noNulls());
  testConstantMap<velox::StringView, int16_t>(noNulls());
  testConstantMap<velox::StringView, int32_t>(noNulls());
  testConstantMap<velox::StringView, int64_t>(noNulls());
  testConstantMap<velox::StringView, float>(noNulls());
  testConstantMap<velox::StringView, double>(noNulls());
  testConstantMap<velox::StringView, velox::StringView>(noNulls());
  testConstantMap<velox::StringView, velox::Timestamp>(noNulls());

  testConstantMap<velox::Timestamp, bool>(noNulls());
  testConstantMap<velox::Timestamp, int8_t>(noNulls());
  testConstantMap<velox::Timestamp, int16_t>(noNulls());
  testConstantMap<velox::Timestamp, int32_t>(noNulls());
  testConstantMap<velox::Timestamp, int64_t>(noNulls());
  testConstantMap<velox::Timestamp, float>(noNulls());
  testConstantMap<velox::Timestamp, double>(noNulls());
  testConstantMap<velox::Timestamp, velox::StringView>(noNulls());
  testConstantMap<velox::Timestamp, velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, ConstantMapSomeNull) {
  testConstantMap<bool, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, int16_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, int32_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, int64_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<bool, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<int8_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int8_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<int16_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int16_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<int32_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int32_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<int64_t, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<int64_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<float, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, int8_t>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, double>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<float, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<double, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<double, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<velox::StringView, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::StringView, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testConstantMap<velox::Timestamp, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testConstantMap<velox::Timestamp, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));
}

TEST_F(RawSizeTestFixture, DictionaryMap) {
  testDictionaryMap<bool, bool>(noNulls());
  testDictionaryMap<bool, int8_t>(noNulls());
  testDictionaryMap<bool, int16_t>(noNulls());
  testDictionaryMap<bool, int32_t>(noNulls());
  testDictionaryMap<bool, int64_t>(noNulls());
  testDictionaryMap<bool, float>(noNulls());
  testDictionaryMap<bool, double>(noNulls());
  testDictionaryMap<bool, velox::StringView>(noNulls());
  testDictionaryMap<bool, velox::Timestamp>(noNulls());

  testDictionaryMap<int8_t, bool>(noNulls());
  testDictionaryMap<int8_t, int8_t>(noNulls());
  testDictionaryMap<int8_t, int16_t>(noNulls());
  testDictionaryMap<int8_t, int32_t>(noNulls());
  testDictionaryMap<int8_t, int64_t>(noNulls());
  testDictionaryMap<int8_t, float>(noNulls());
  testDictionaryMap<int8_t, double>(noNulls());
  testDictionaryMap<int8_t, velox::StringView>(noNulls());
  testDictionaryMap<int8_t, velox::Timestamp>(noNulls());

  testDictionaryMap<int16_t, bool>(noNulls());
  testDictionaryMap<int16_t, int8_t>(noNulls());
  testDictionaryMap<int16_t, int16_t>(noNulls());
  testDictionaryMap<int16_t, int32_t>(noNulls());
  testDictionaryMap<int16_t, int64_t>(noNulls());
  testDictionaryMap<int16_t, float>(noNulls());
  testDictionaryMap<int16_t, double>(noNulls());
  testDictionaryMap<int16_t, velox::StringView>(noNulls());
  testDictionaryMap<int16_t, velox::Timestamp>(noNulls());

  testDictionaryMap<int32_t, bool>(noNulls());
  testDictionaryMap<int32_t, int8_t>(noNulls());
  testDictionaryMap<int32_t, int16_t>(noNulls());
  testDictionaryMap<int32_t, int32_t>(noNulls());
  testDictionaryMap<int32_t, int64_t>(noNulls());
  testDictionaryMap<int32_t, float>(noNulls());
  testDictionaryMap<int32_t, double>(noNulls());
  testDictionaryMap<int32_t, velox::StringView>(noNulls());
  testDictionaryMap<int32_t, velox::Timestamp>(noNulls());

  testDictionaryMap<int64_t, bool>(noNulls());
  testDictionaryMap<int64_t, int8_t>(noNulls());
  testDictionaryMap<int64_t, int16_t>(noNulls());
  testDictionaryMap<int64_t, int32_t>(noNulls());
  testDictionaryMap<int64_t, int64_t>(noNulls());
  testDictionaryMap<int64_t, float>(noNulls());
  testDictionaryMap<int64_t, double>(noNulls());
  testDictionaryMap<int64_t, velox::StringView>(noNulls());
  testDictionaryMap<int64_t, velox::Timestamp>(noNulls());

  testDictionaryMap<float, bool>(noNulls());
  testDictionaryMap<float, int8_t>(noNulls());
  testDictionaryMap<float, int16_t>(noNulls());
  testDictionaryMap<float, int32_t>(noNulls());
  testDictionaryMap<float, int64_t>(noNulls());
  testDictionaryMap<float, float>(noNulls());
  testDictionaryMap<float, double>(noNulls());
  testDictionaryMap<float, velox::StringView>(noNulls());
  testDictionaryMap<float, velox::Timestamp>(noNulls());

  testDictionaryMap<double, bool>(noNulls());
  testDictionaryMap<double, int8_t>(noNulls());
  testDictionaryMap<double, int16_t>(noNulls());
  testDictionaryMap<double, int32_t>(noNulls());
  testDictionaryMap<double, int64_t>(noNulls());
  testDictionaryMap<double, float>(noNulls());
  testDictionaryMap<double, double>(noNulls());
  testDictionaryMap<double, velox::StringView>(noNulls());
  testDictionaryMap<double, velox::Timestamp>(noNulls());

  testDictionaryMap<velox::StringView, bool>(noNulls());
  testDictionaryMap<velox::StringView, int8_t>(noNulls());
  testDictionaryMap<velox::StringView, int16_t>(noNulls());
  testDictionaryMap<velox::StringView, int32_t>(noNulls());
  testDictionaryMap<velox::StringView, int64_t>(noNulls());
  testDictionaryMap<velox::StringView, float>(noNulls());
  testDictionaryMap<velox::StringView, double>(noNulls());
  testDictionaryMap<velox::StringView, velox::StringView>(noNulls());
  testDictionaryMap<velox::StringView, velox::Timestamp>(noNulls());

  testDictionaryMap<velox::Timestamp, bool>(noNulls());
  testDictionaryMap<velox::Timestamp, int8_t>(noNulls());
  testDictionaryMap<velox::Timestamp, int16_t>(noNulls());
  testDictionaryMap<velox::Timestamp, int32_t>(noNulls());
  testDictionaryMap<velox::Timestamp, int64_t>(noNulls());
  testDictionaryMap<velox::Timestamp, float>(noNulls());
  testDictionaryMap<velox::Timestamp, double>(noNulls());
  testDictionaryMap<velox::Timestamp, velox::StringView>(noNulls());
  testDictionaryMap<velox::Timestamp, velox::Timestamp>(noNulls());
}

TEST_F(RawSizeTestFixture, DictionaryMapSomeNull) {
  testDictionaryMap<bool, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, float>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<bool, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<int8_t, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int8_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<int16_t, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int16_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<int32_t, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int32_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<int64_t, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<int64_t, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<float, bool>(randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<float, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<double, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<double, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<velox::StringView, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));

  testDictionaryMap<velox::Timestamp, bool>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, int8_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, int16_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, int32_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, int64_t>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, float>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, double>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::StringView, velox::StringView>(
      randomNulls(folly::Random::rand32() % 10 + 1));
  testDictionaryMap<velox::Timestamp, velox::Timestamp>(
      randomNulls(folly::Random::rand32() % 10 + 1));
}

/*
 * The following tests are considered handcrafted tests for different nested
 * vector cases. These tests cases have specified expected sizes.
 */
TEST_F(RawSizeTestFixture, ArrayNested) {
  auto arrayVector =
      vectorMaker_->arrayVector<int64_t>({{0, 1}, {1, 0, 1}, {0}});
  auto arrayVector2 = vectorMaker_->arrayVector<int64_t>({{1}, {1, 0}, {0}});
  auto nestedArrayVector = vectorMaker_->arrayVector({0, 1}, arrayVector);
  auto nestedArrayVector2 = vectorMaker_->arrayVector({0, 2}, arrayVector2);
  nestedArrayVector->append(nestedArrayVector2.get());
  this->ranges_.add(0, nestedArrayVector->size());
  auto rawSize = nimble::getRawSizeFromVector(nestedArrayVector, this->ranges_);
  constexpr auto expectedRawSize = sizeof(int64_t) * 10;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, MapNested) {
  auto mapKeys = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee"}); // 15
  auto mapValues = vectorMaker_->flatVector<int64_t>({0, 1, 2, 3, 4}); // 40

  auto mapKeys2 = vectorMaker_->mapVector({0, 1, 3}, mapKeys, mapValues);
  auto mapValues2 = vectorMaker_->flatVector<int64_t>({0, 1, 2}); //  24

  auto mapVector = vectorMaker_->mapVector({0, 1}, mapKeys2, mapValues2);

  auto mapKeys3 = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd"}); // 10
  auto mapValues3 = vectorMaker_->flatVector<int64_t>({0, 1, 2, 3}); // 32

  auto mapKeys4 = vectorMaker_->mapVector({0, 1, 3}, mapKeys3, mapValues3);
  auto mapValues4 = vectorMaker_->flatVector<int64_t>({0, 1, 2}); // 24

  auto mapVector2 = vectorMaker_->mapVector({0, 1}, mapKeys4, mapValues4);

  mapVector->append(mapVector2.get());
  this->ranges_.add(0, mapVector->size());
  auto rawSize = nimble::getRawSizeFromVector(mapVector, this->ranges_);
  constexpr auto expectedSize = sizeof(int64_t) * 15 + 25;

  ASSERT_EQ(expectedSize, rawSize);
}

TEST_F(RawSizeTestFixture, MapArrayNested) {
  auto mapKeys = vectorMaker_->arrayVector<int64_t>(
      {{0, 1, 0}, {1, 0, 1}, {0, 1}, {0}, {1, 0, 1, 0}});
  auto mapValues = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee"});

  auto mapKeys2 = vectorMaker_->mapVector({0, 1, 3}, mapKeys, mapValues);
  auto mapValues2 = vectorMaker_->flatVector<int64_t>({0, 1, 2});

  auto mapVector = vectorMaker_->mapVector({0, 1}, mapKeys2, mapValues2);
  this->ranges_.add(0, mapVector->size());
  auto rawSize = nimble::getRawSizeFromVector(mapVector, this->ranges_);
  constexpr auto expectedSize = sizeof(int64_t) * 16 + 15;

  ASSERT_EQ(expectedSize, rawSize);
}

TEST_F(RawSizeTestFixture, ArrayMapNested) {
  auto mapKeys = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee"});
  auto mapValues = vectorMaker_->flatVector<int64_t>({0, 1, 2, 3, 4});

  auto mapVector = vectorMaker_->mapVector({0, 1, 3}, mapKeys, mapValues);
  auto nestedArrayVector = vectorMaker_->arrayVector({0, 1}, mapVector);

  this->ranges_.add(0, nestedArrayVector->size());
  auto rawSize = nimble::getRawSizeFromVector(nestedArrayVector, this->ranges_);
  constexpr auto expectedSize = sizeof(int64_t) * 5 + 15;

  ASSERT_EQ(expectedSize, rawSize);
}

TEST_F(RawSizeTestFixture, RowSameTypes) {
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<int64_t>({0, 1, 0, 1, 0, 1});
  auto childVector3 = vectorMaker_->flatVector<int64_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});
  this->ranges_.add(0, rowVector->size());
  auto rawSize = nimble::getRawSizeFromVector(rowVector, this->ranges_);

  ASSERT_EQ(sizeof(int64_t) * 18, rawSize);
}

TEST_F(RawSizeTestFixture, RowDifferentTypes) {
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<bool>({0, 1, 0, 1, 0, 1});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});
  this->ranges_.add(0, rowVector->size());
  auto rawSize = nimble::getRawSizeFromVector(rowVector, this->ranges_);
  constexpr auto expectedRawSize =
      sizeof(int64_t) * 6 + sizeof(bool) * 6 + sizeof(int16_t) * 6;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, RowDifferentTypes2) {
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});
  this->ranges_.add(0, rowVector->size());
  auto rawSize = nimble::getRawSizeFromVector(rowVector, this->ranges_);
  constexpr auto expectedRawSize =
      sizeof(int64_t) * 6 + sizeof(int16_t) * 6 + 21;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, RowNulls) {
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<bool>({0, 1, 0, 1, 0, 1});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  velox::BufferPtr nulls = velox::AlignedBuffer::allocate<bool>(
      6, this->pool_.get(), velox::bits::kNotNull);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  velox::bits::setNull(rawNulls, 2);
  const std::vector<velox::VectorPtr>& children = {
      childVector1, childVector2, childVector3};
  auto rowVector = std::make_shared<velox::RowVector>(
      pool_.get(),
      velox::ROW({velox::BIGINT(), velox::BOOLEAN(), velox::SMALLINT()}),
      nulls,
      6,
      children);
  this->ranges_.add(0, rowVector->size());
  auto rawSize = nimble::getRawSizeFromVector(rowVector, this->ranges_);
  constexpr auto expectedRawSize = sizeof(int64_t) * 5 + sizeof(bool) * 5 +
      sizeof(int16_t) * 5 + nimble::NULL_SIZE * 1;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, RowNestedNull) {
  auto childVector1 =
      vectorMaker_->flatVectorNullable<int64_t>({0, 0, std::nullopt, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVectorNullable<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", std::nullopt});
  auto childVector3 =
      vectorMaker_->flatVectorNullable<int16_t>({0, 1, 0, 1, 0, std::nullopt});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});
  this->ranges_.add(0, rowVector->size());
  auto rawSize = nimble::getRawSizeFromVector(rowVector, this->ranges_);
  constexpr auto expectedRawSize = sizeof(int64_t) * 5 + (1 + 2 + 3 + 4 + 5) +
      sizeof(int16_t) * 5 + nimble::NULL_SIZE * 3;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, RowDictionaryChildren) {
  auto arrayVector =
      vectorMaker_->arrayVector<int64_t>({{0, 1, 2}, {3, 4}, {5}});
  std::unordered_map<velox::vector_size_t, uint64_t> indexToSizeArray;
  indexToSizeArray[0] = sizeof(int64_t) * 3; // Size for the first row
  indexToSizeArray[1] = sizeof(int64_t) * 2; // Size for the second row
  indexToSizeArray[2] = sizeof(int64_t) * 1; // Size for the third row

  uint64_t expectedArrayRawSize = 0;
  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          VECTOR_SIZE, this->pool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  for (int i = 0; i < VECTOR_SIZE; i++) {
    rawIndices[i] = i % 3;
    expectedArrayRawSize += indexToSizeArray[i % 3];
  }

  // Create a dictionary vector of size VECTOR_SIZE containing no nulls
  auto dictArrayVector = velox::BaseVector::wrapInDictionary(
      velox::BufferPtr(nullptr), indices, VECTOR_SIZE, arrayVector);

  auto mapKeys = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee"});
  auto mapValues = vectorMaker_->flatVector<velox::StringView>(
      {"eeeee", "dddd", "ccc", "bb", "a"});

  auto mapVector = vectorMaker_->mapVector({0, 1, 3}, mapKeys, mapValues);
  std::unordered_map<velox::vector_size_t, uint64_t> indexToSizeMap;
  indexToSizeMap[0] = 6; // Size for the first row
  indexToSizeMap[1] = 12; // Size for the second row
  indexToSizeMap[2] = 12; // Size for the third row

  uint64_t expectedMapRawSize = 0;
  velox::BufferPtr indices2 =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          VECTOR_SIZE, this->pool_.get());
  auto* rawIndices2 = indices2->asMutable<velox::vector_size_t>();
  for (int i = 0; i < VECTOR_SIZE; i++) {
    rawIndices2[i] = i % 3;
    expectedMapRawSize += indexToSizeMap[i % 3];
  }

  auto dictMapVector = velox::BaseVector::wrapInDictionary(
      velox::BufferPtr(nullptr), indices, VECTOR_SIZE, mapVector);
  auto rowVector =
      vectorMaker_->rowVector({"1", "2"}, {dictArrayVector, dictMapVector});

  this->ranges_.add(0, rowVector->size());
  auto rawSize = nimble::getRawSizeFromVector(rowVector, this->ranges_);
  const uint64_t expectedSize = expectedArrayRawSize + expectedMapRawSize;

  ASSERT_EQ(expectedSize, rawSize);
}

TEST_F(RawSizeTestFixture, ConstRow) {
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});
  auto constVector = velox::BaseVector::wrapInConstant(10, 5, rowVector);
  this->ranges_.add(0, constVector->size());
  auto rawSize = nimble::getRawSizeFromVector(constVector, this->ranges_);
  constexpr auto expectedRawSize = (sizeof(int64_t) + 6 + sizeof(int16_t)) * 10;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, ConstRowNestedNull) {
  auto childVector1 =
      vectorMaker_->flatVectorNullable<int64_t>({0, 0, std::nullopt, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVectorNullable<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", std::nullopt});
  auto childVector3 =
      vectorMaker_->flatVectorNullable<int16_t>({0, 1, 0, 1, 0, std::nullopt});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});
  auto constVector = velox::BaseVector::wrapInConstant(10, 5, rowVector);
  this->ranges_.add(0, constVector->size());
  auto rawSize = nimble::getRawSizeFromVector(constVector, this->ranges_);
  constexpr auto expectedRawSize =
      (sizeof(int64_t) + nimble::NULL_SIZE * 2) * 10;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, DictRow) {
  constexpr velox::vector_size_t VECTOR_TEST_SIZE = 5;
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});

  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          VECTOR_TEST_SIZE, this->pool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 0;
  rawIndices[2] = 1;
  rawIndices[3] = 2;
  rawIndices[4] = 3;

  // Create a dictionary vector of size VECTOR_TEST_SIZE containing no nulls
  auto dictVector = velox::BaseVector::wrapInDictionary(
      velox::BufferPtr(nullptr), indices, VECTOR_TEST_SIZE, rowVector);
  this->ranges_.add(0, dictVector->size());
  auto rawSize = nimble::getRawSizeFromVector(dictVector, this->ranges_);
  constexpr auto expectedRawSize =
      sizeof(int64_t) * 5 + sizeof(int16_t) * 5 + 11;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, DictRowNull) {
  constexpr velox::vector_size_t VECTOR_TEST_SIZE = 5;
  auto childVector1 =
      vectorMaker_->flatVectorNullable<int64_t>({std::nullopt, 0, 0, 1, 1, 0});
  auto childVector2 = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});

  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          VECTOR_TEST_SIZE, this->pool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 0;
  rawIndices[2] = 1;
  rawIndices[3] = 2;
  rawIndices[4] = 3;

  // Create a dictionary vector of size VECTOR_TEST_SIZE containing no nulls
  auto dictVector = velox::BaseVector::wrapInDictionary(
      velox::BufferPtr(nullptr), indices, VECTOR_TEST_SIZE, rowVector);
  this->ranges_.add(0, dictVector->size());
  auto rawSize = nimble::getRawSizeFromVector(dictVector, this->ranges_);
  constexpr auto expectedRawSize =
      sizeof(int64_t) * 3 + sizeof(int16_t) * 5 + 11 + nimble::NULL_SIZE * 2;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, DictRowNullTopLevel) {
  constexpr velox::vector_size_t VECTOR_TEST_SIZE = 5;
  auto childVector1 = vectorMaker_->flatVector<int64_t>({0, 0, 0, 1, 1, 1});
  auto childVector2 = vectorMaker_->flatVector<velox::StringView>(
      {"a", "bb", "ccc", "dddd", "eeeee", "ffffff"});
  auto childVector3 = vectorMaker_->flatVector<int16_t>({0, 1, 0, 1, 0, 1});
  auto rowVector = vectorMaker_->rowVector(
      {"1", "2", "3"}, {childVector1, childVector2, childVector3});

  velox::BufferPtr indices =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(
          VECTOR_TEST_SIZE, this->pool_.get());
  auto* rawIndices = indices->asMutable<velox::vector_size_t>();
  rawIndices[0] = 0;
  rawIndices[1] = 0;
  rawIndices[2] = 1; // null
  rawIndices[3] = 2;
  rawIndices[4] = 3;

  velox::BufferPtr nulls = velox::AlignedBuffer::allocate<bool>(
      VECTOR_TEST_SIZE, this->pool_.get(), velox::bits::kNotNull);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  velox::bits::setNull(rawNulls, 2);

  // Create a dictionary vector of size VECTOR_TEST_SIZE containing no nulls
  auto dictVector = velox::BaseVector::wrapInDictionary(
      nulls, indices, VECTOR_TEST_SIZE, rowVector);
  this->ranges_.add(0, dictVector->size());
  auto rawSize = nimble::getRawSizeFromVector(dictVector, this->ranges_);
  constexpr auto expectedRawSize =
      sizeof(int64_t) * 4 + sizeof(int16_t) * 4 + 9 + nimble::NULL_SIZE * 1;

  ASSERT_EQ(expectedRawSize, rawSize);
}

TEST_F(RawSizeTestFixture, ThrowOnDefaultType) {
  auto unknownVector = facebook::velox::BaseVector::create(
      facebook::velox::UNKNOWN(), 10, pool_.get());
  this->ranges_.add(0, unknownVector->size());

  EXPECT_THROW(
      nimble::getRawSizeFromVector(unknownVector, this->ranges_),
      velox::VeloxRuntimeError);
}

TEST_F(RawSizeTestFixture, ThrowOnDefaultEncodingFixedWidth) {
  auto sequenceVector =
      vectorMaker_->sequenceVector<int8_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  this->ranges_.add(0, sequenceVector->size());

  EXPECT_THROW(
      nimble::getRawSizeFromVector(sequenceVector, this->ranges_),
      velox::VeloxRuntimeError);
}

TEST_F(RawSizeTestFixture, ThrowOnDefaultEncodingVariableWidth) {
  auto sequenceVector = vectorMaker_->sequenceVector<velox::StringView>(
      {"a", "bbbb", "ccccccccc", "dddddddddddddddd"});
  this->ranges_.add(0, sequenceVector->size());

  EXPECT_THROW(
      nimble::getRawSizeFromVector(sequenceVector, this->ranges_),
      velox::VeloxRuntimeError);
}

TEST_F(RawSizeTestFixture, LocalDecodedVectorMoveConstructor) {
  facebook::nimble::RawSizeContext context;

  auto localDecodedVector1 =
      facebook::nimble::DecodedVectorManager::LocalDecodedVector(
          context.getDecodedVectorManager());

  // Constuct LocalDecodedVector by LocalDecodedVector ctr
  auto localDecodedVector2 = std::move(localDecodedVector1);

  // Access the DecodedVector to ensure it's not null
  velox::DecodedVector& decodedVector = localDecodedVector2.get();
  ASSERT_NE(&decodedVector, nullptr);

  // No checks on localDecodedVector1 as it's been moved
}
