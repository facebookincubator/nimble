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
#include "dwio/nimble/velox/FieldReader.h"
#include <velox/type/StringView.h>
#include <cstddef>
#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::nimble {

namespace {

constexpr uint32_t kSkipBatchSize = 1024;

uint32_t scatterCount(uint32_t count, const bits::Bitmap* scatterBitmap) {
  return scatterBitmap ? scatterBitmap->size() : count;
}

// Bytes needed for a packed null bitvector in velox.
constexpr uint64_t nullBytes(uint32_t rowCount) {
  return velox::bits::nbytes(rowCount) + velox::simd::kPadding;
}

// Bits needed for a packed null bitvector in velox.
constexpr uint64_t nullBits(uint32_t rowCount) {
  return rowCount + 8 * velox::simd::kPadding;
}

// Returns the nulls for a vector properly padded to hold |rowCount|.
char* paddedNulls(velox::BaseVector* vec, uint32_t rowCount) {
  return vec->mutableNulls(nullBits(rowCount))->template asMutable<char>();
}

// Zeroes vec's null vector (aka make it 'all null').
void zeroNulls(velox::BaseVector* vec, uint32_t rowCount) {
  memset(paddedNulls(vec, rowCount), 0, nullBytes(rowCount));
}

template <typename T>
T* FOLLY_NULLABLE verifyVectorState(velox::VectorPtr& vector) {
  // we want vector to not be referenced by anyone else (e.g. ref count of 1)
  if (vector) {
    auto casted = vector->as<T>();
    if (casted && vector.use_count() == 1) {
      return casted;
    }
    vector.reset();
  }
  return nullptr;
}

// Ensure the internal buffer to vector are refCounted to one
template <typename... T>
inline void resetIfNotWritable(velox::VectorPtr& vector, T&... buffer) {
  // The result vector and the buffer both hold reference, so refCount is at
  // least 2
  auto resetIfShared = [](auto& buffer) {
    if (!buffer) {
      return false;
    }
    const bool reset = buffer->refCount() > 2;
    if (reset) {
      buffer.reset();
    }
    return reset;
  };

  if ((... || resetIfShared(buffer))) {
    vector.reset();
  }
}

template <typename T>
struct VectorInitializer {};

template <typename T>
struct VectorInitializer<velox::FlatVector<T>> {
  static velox::FlatVector<T>* initialize(
      velox::memory::MemoryPool* pool,
      velox::VectorPtr& output,
      const velox::TypePtr& veloxType,
      uint64_t rowCount,
      velox::BufferPtr values = nullptr) {
    auto vector = verifyVectorState<velox::FlatVector<T>>(output);
    velox::BufferPtr nulls;
    if (vector) {
      nulls = vector->nulls();
      values = vector->mutableValues(rowCount);
      resetIfNotWritable(output, nulls, values);
    }
    if (!values) {
      values = velox::AlignedBuffer::allocate<T>(rowCount, pool);
    }
    if (!output) {
      output = std::make_shared<velox::FlatVector<T>>(
          pool,
          veloxType,
          nulls,
          rowCount,
          values,
          std::vector<velox::BufferPtr>());
    }
    return static_cast<velox::FlatVector<T>*>(output.get());
  }
};

template <>
struct VectorInitializer<velox::ArrayVector> {
  static velox::ArrayVector* initialize(
      velox::memory::MemoryPool* pool,
      velox::VectorPtr& output,
      const velox::TypePtr& veloxType,
      uint64_t rowCount) {
    auto vector = verifyVectorState<velox::ArrayVector>(output);
    velox::BufferPtr nulls, sizes, offsets;
    velox::VectorPtr elements;
    if (vector) {
      nulls = vector->nulls();
      sizes = vector->mutableSizes(rowCount);
      offsets = vector->mutableOffsets(rowCount);
      elements = vector->elements();
      resetIfNotWritable(output, nulls, sizes, offsets);
    }
    if (!offsets) {
      offsets =
          velox::AlignedBuffer::allocate<velox::vector_size_t>(rowCount, pool);
    }
    if (!sizes) {
      sizes =
          velox::AlignedBuffer::allocate<velox::vector_size_t>(rowCount, pool);
    }
    if (!output) {
      output = std::make_shared<velox::ArrayVector>(
          pool,
          veloxType,
          nulls,
          rowCount,
          std::move(offsets),
          std::move(sizes),
          /* elements */ elements,
          0 /*nullCount*/);
    }
    return static_cast<velox::ArrayVector*>(output.get());
  }
};

template <>
struct VectorInitializer<velox::MapVector> {
  static velox::MapVector* initialize(
      velox::memory::MemoryPool* pool,
      velox::VectorPtr& output,
      const velox::TypePtr& veloxType,
      uint64_t rowCount) {
    auto vector = verifyVectorState<velox::MapVector>(output);
    velox::BufferPtr nulls, sizes, offsets;
    velox::VectorPtr mapKeys, mapValues;
    if (vector) {
      nulls = vector->nulls();
      sizes = vector->mutableSizes(rowCount);
      offsets = vector->mutableOffsets(rowCount);
      mapKeys = vector->mapKeys();
      mapValues = vector->mapValues();
      resetIfNotWritable(output, nulls, sizes, offsets);
    }
    if (!offsets) {
      offsets =
          velox::AlignedBuffer::allocate<velox::vector_size_t>(rowCount, pool);
    }
    if (!sizes) {
      sizes =
          velox::AlignedBuffer::allocate<velox::vector_size_t>(rowCount, pool);
    }
    if (!output) {
      output = std::make_shared<velox::MapVector>(
          pool,
          veloxType,
          nulls,
          rowCount,
          std::move(offsets),
          std::move(sizes),
          /* keys*/ mapKeys,
          /*values*/ mapValues,
          0 /*nullCount*/);
    }
    return static_cast<velox::MapVector*>(output.get());
  }
};

template <>
struct VectorInitializer<velox::RowVector> {
  static velox::RowVector* initialize(
      velox::memory::MemoryPool* pool,
      velox::VectorPtr& output,
      const velox::TypePtr& veloxType,
      uint64_t rowCount) {
    auto vector = verifyVectorState<velox::RowVector>(output);
    velox::BufferPtr nulls;
    std::vector<velox::VectorPtr> childrenVectors;
    if (vector) {
      nulls = vector->nulls();
      childrenVectors = vector->children();
      resetIfNotWritable(output, nulls);
    } else {
      childrenVectors.resize(veloxType->size());
    }
    if (!output) {
      output = std::make_shared<velox::RowVector>(
          pool,
          veloxType,
          nulls,
          rowCount,
          std::move(childrenVectors),
          0 /*nullCount*/);
    }
    return static_cast<velox::RowVector*>(output.get());
  }
};

class NullColumnReader final : public FieldReader {
 public:
  NullColumnReader(velox::memory::MemoryPool& pool, velox::TypePtr type)
      : FieldReader{pool, std::move(type), nullptr} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    return std::optional<std::pair<uint32_t, uint64_t>>({0, 0});
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    ensureNullConstant(scatterCount(count, scatterBitmap), output, type_);
  }

  void skip(uint32_t /* count */) final {}
};

class NullFieldReaderFactory final : public FieldReaderFactory {
 public:
  NullFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType)
      : FieldReaderFactory{pool, std::move(veloxType), nullptr} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<
          offset_size,
          std::unique_ptr<Decoder>>& /* decoders */) final {
    return createNullColumnReader();
  }
};

template <typename T>
static auto wrap(T& t) {
  return [&]() -> T& { return t; };
}

template <typename TRequested, typename TData>
struct IsBool : std::false_type {};

template <>
struct IsBool<bool, bool> : std::true_type {};

template <typename TRequested, typename TData, typename = void>
struct ScalarFieldReaderBase;

template <typename TRequested, typename TData>
struct ScalarFieldReaderBase<
    TRequested,
    TData,
    std::enable_if_t<IsBool<TRequested, TData>::value>> {
  explicit ScalarFieldReaderBase(velox::memory::MemoryPool& pool)
      : buf_{&pool} {}

  bool* ensureBuffer(uint32_t rowCount) {
    buf_.reserve(rowCount);
    auto data = buf_.data();
    std::fill(data, data + rowCount, false);
    return data;
  }

  Vector<bool> buf_;
};

template <typename TRequested, typename TData>
struct ScalarFieldReaderBase<
    TRequested,
    TData,
    std::enable_if_t<!IsBool<TRequested, TData>::value>> {
  explicit ScalarFieldReaderBase(velox::memory::MemoryPool& /* pool */) {}
};

// TRequested is the requested data type from the reader, TData is the
// data type as stored in the file's schema
template <typename TRequested, typename TData>
class ScalarFieldReader final
    : public FieldReader,
      private ScalarFieldReaderBase<TRequested, TData> {
 public:
  ScalarFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder)
      : FieldReader(pool, std::move(type), decoder),
        ScalarFieldReaderBase<TRequested, TData>{pool} {
    if constexpr (
        (isSignedIntegralType<TRequested>() && !isSignedIntegralType<TData>() &&
         !isBoolType<TData>()) ||
        (isUnsignedIntegralType<TRequested>() &&
         !isUnsignedIntegralType<TData>()) ||
        (isFloatingPointType<TRequested>() && !isFloatingPointType<TData>()) ||
        sizeof(TRequested) < sizeof(TData)) {
      NIMBLE_ASSERT(false, "Incompatabile data type and requested type");
    }
  }

  using FieldReader::FieldReader;

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    const auto* encoding = decoder_->encoding();
    NIMBLE_CHECK(
        encoding != nullptr,
        "Decoder must be loaded for output size estimation.");
    const auto rowCount = encoding->rowCount();

    if (encoding->isNullable()) {
      // Adding memory for velox::BaseVector::nulls_
      totalBytes += rowCount / 8;
    }

    NIMBLE_CHECK(
        type_->isPrimitiveType(),
        "Velox type must be primitive in ScalarFieldReader");
    NIMBLE_CHECK(
        type_->isFixedWidth(),
        "Velox type must be fixed width in ScalarFieldReader");
    const auto veloxType = type_->kind();

    switch (veloxType) {
      case velox::TypeKind::BOOLEAN:
        // Bit packed representation for bool type
        totalBytes += rowCount / 8;
        break;
      case velox::TypeKind::TINYINT:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::TINYINT>::NativeType);
        break;
      case velox::TypeKind::SMALLINT:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::SMALLINT>::NativeType);
        break;
      case velox::TypeKind::INTEGER:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType);
        break;
      case velox::TypeKind::BIGINT:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType);
        break;
      case velox::TypeKind::REAL:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::REAL>::NativeType);
        break;
      case velox::TypeKind::DOUBLE:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::DOUBLE>::NativeType);
        break;
      case velox::TypeKind::TIMESTAMP:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::TIMESTAMP>::NativeType);
        break;
      case velox::TypeKind::HUGEINT:
        totalBytes += rowCount *
            sizeof(velox::TypeTraits<velox::TypeKind::HUGEINT>::NativeType);
        break;
      default:
        return std::nullopt;
    }
    return std::optional<std::pair<uint32_t, uint64_t>>(
        {rowCount, totalBytes / rowCount});
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    const auto rowCount = scatterCount(count, scatterBitmap);
    auto vector = VectorInitializer<velox::FlatVector<TRequested>>::initialize(
        &pool_, output, type_, rowCount);
    vector->resize(rowCount);

    auto upcastNoNulls = [&vector]() {
      auto vecRowCount = vector->size();
      if (vecRowCount == 0) {
        return;
      }
      auto to = vector->mutableRawValues();
      const auto from = vector->template rawValues<TData>();
      // we can't use for (uint32_t i = vecRowCount - 1; i >= 0; --i)
      // for the loop control, because for unsigned int, i >= 0 is always true,
      // it becomes an infinite loop
      for (uint32_t i = 0; i < vecRowCount; ++i) {
        to[vecRowCount - i - 1] =
            static_cast<TRequested>(from[vecRowCount - i - 1]);
      }
    };

    auto upcastWithNulls = [&vector]() {
      auto vecRowCount = vector->size();
      if (vecRowCount == 0) {
        return;
      }
      auto to = vector->mutableRawValues();
      const auto from = vector->template rawValues<TData>();
      for (uint32_t i = 0; i < vecRowCount; ++i) {
        if (vector->isNullAt(vecRowCount - i - 1)) {
          to[vecRowCount - i - 1] = TRequested();
        } else {
          to[vecRowCount - i - 1] =
              static_cast<TRequested>(from[vecRowCount - i - 1]);
        }
      }
    };

    uint32_t nonNullCount = 0;
    if constexpr (IsBool<TRequested, TData>::value) {
      // TODO: implement method for bitpacked bool
      auto buf = this->ensureBuffer(rowCount);
      nonNullCount = decoder_->next(
          count,
          buf,
          [&]() { return paddedNulls(vector, rowCount); },
          scatterBitmap);

      auto target = vector->mutableValues(rowCount)->template asMutable<char>();
      std::fill(target, target + bits::bytesRequired(rowCount), 0);
      for (uint32_t i = 0; i < rowCount; ++i) {
        bits::maybeSetBit(i, target, buf[i]);
      }
    } else {
      nonNullCount = decoder_->next(
          count,
          vector->mutableValues(rowCount)->template asMutable<TRequested>(),
          [&]() { return paddedNulls(vector, rowCount); },
          scatterBitmap);
    }

    if (nonNullCount == rowCount) {
      vector->resetNulls();
      if constexpr (sizeof(TRequested) > sizeof(TData)) {
        upcastNoNulls();
      }
    } else {
      vector->setNullCount(rowCount - nonNullCount);
      if constexpr (sizeof(TRequested) > sizeof(TData)) {
        upcastWithNulls();
      }
    }
  }

  void skip(uint32_t count) final {
    decoder_->skip(count);
  }
};

template <typename T>
class ScalarFieldReaderFactory final : public FieldReaderFactory {
 public:
  ScalarFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type)
      : FieldReaderFactory{pool, std::move(veloxType), type} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    const auto& descriptor = nimbleType_->asScalar().scalarDescriptor();
    switch (descriptor.scalarKind()) {
      case ScalarKind::Bool: {
        return createReaderImpl<ScalarFieldReader<T, bool>>(
            decoders, descriptor);
      }
      case ScalarKind::Int8: {
        return createReaderImpl<ScalarFieldReader<T, int8_t>>(
            decoders, descriptor);
      }
      case ScalarKind::Int16: {
        return createReaderImpl<ScalarFieldReader<T, int16_t>>(
            decoders, descriptor);
      }
      case ScalarKind::Int32: {
        return createReaderImpl<ScalarFieldReader<T, int32_t>>(
            decoders, descriptor);
      }
      case ScalarKind::Int64: {
        return createReaderImpl<ScalarFieldReader<T, int64_t>>(
            decoders, descriptor);
      }
      case ScalarKind::Float: {
        return createReaderImpl<ScalarFieldReader<T, float>>(
            decoders, descriptor);
      }
      case ScalarKind::Double: {
        return createReaderImpl<ScalarFieldReader<T, double>>(
            decoders, descriptor);
      }
      case ScalarKind::UInt8:
      case ScalarKind::UInt16:
      case ScalarKind::UInt32: {
        return createReaderImpl<ScalarFieldReader<T, uint32_t>>(
            decoders, descriptor);
      }
      case ScalarKind::UInt64:
      case ScalarKind::String:
      case ScalarKind::Binary:
      case ScalarKind::Undefined: {
        NIMBLE_NOT_SUPPORTED(fmt::format(
            "Unsupported nimble scalar type: {}.",
            toString(descriptor.scalarKind())))
      }
    }
    NIMBLE_UNREACHABLE(fmt::format(
        "Should not have nimble scalar type: {}.",
        toString(descriptor.scalarKind())))
  }
};

class StringFieldReader final : public FieldReader {
 public:
  StringFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::vector<std::string_view>& buffer)
      : FieldReader{pool, std::move(type), decoder}, buffer_{buffer} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    const auto* encoding = decoder_->encoding();
    NIMBLE_CHECK(
        encoding != nullptr,
        "Decoder must be loaded for output size estimation.");
    const auto* innerEncoding = encoding;
    const auto rowCount = encoding->rowCount();

    if (encoding->isNullable()) {
      // Adding memory for velox::BaseVector::nulls_
      totalBytes += rowCount / 8;
      const auto* nullableEncoding =
          dynamic_cast<const NullableEncoding<std::string_view>*>(encoding);
      NIMBLE_CHECK(
          nullableEncoding != nullptr,
          "NullableEncoding is not used for nullable string field.");
      innerEncoding = nullableEncoding->nonNulls();
    }

    // TODO: support more encodings (or do encoding traversal), DICT, RLE, etc.
    // We currently only estimate trivial encoded string field.
    if (const auto* trivialEncoding =
            dynamic_cast<const TrivialEncoding<std::string_view>*>(
                innerEncoding)) {
      // Adding overhead for velox::StringView. 4 bytes for inline, 16 bytes for
      // non-inline
      const auto nonNullCount = trivialEncoding->rowCount();
      const auto payloadBytes = trivialEncoding->uncompressedDataBytes();
      // Non-null entries overhead
      totalBytes +=
          ((payloadBytes / nonNullCount) > velox::StringView::kInlineSize ? 16
                                                                          : 4) *
          nonNullCount;
      // Null entries overhead
      totalBytes += (rowCount - nonNullCount) * 16;

      // Adding actual string content payload size
      totalBytes += payloadBytes;
    } else {
      return std::nullopt;
    }

    return rowCount == 0 ? std::optional<std::pair<uint32_t, uint64_t>>({0, 0})
                         : std::optional<std::pair<uint32_t, uint64_t>>(
                               {rowCount, totalBytes / rowCount});
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    auto rowCount = scatterCount(count, scatterBitmap);
    auto vector =
        VectorInitializer<velox::FlatVector<velox::StringView>>::initialize(
            &pool_, output, type_, rowCount);
    vector->clearStringBuffers();
    vector->resize(rowCount);
    buffer_.resize(rowCount);

    auto nonNullCount = decoder_->next(
        count,
        buffer_.data(),
        [&]() { return paddedNulls(vector, rowCount); },
        scatterBitmap);

    if (nonNullCount == rowCount) {
      vector->resetNulls();
      for (uint32_t i = 0; i < rowCount; ++i) {
        vector->set(
            i,
            // @lint-ignore CLANGTIDY facebook-hte-MemberUncheckedArrayBounds
            {buffer_[i].data(), static_cast<int32_t>(buffer_[i].length())});
      }
    } else {
      vector->setNullCount(rowCount - nonNullCount);
      for (uint32_t i = 0; i < rowCount; ++i) {
        if (!vector->isNullAt(i)) {
          vector->set(
              i,
              // @lint-ignore CLANGTIDY facebook-hte-MemberUncheckedArrayBounds
              {buffer_[i].data(), static_cast<int32_t>(buffer_[i].length())});
        }
      }
    }
  }

  void skip(uint32_t count) final {
    decoder_->skip(count);
  }

 private:
  std::vector<std::string_view>& buffer_;
};

class StringFieldReaderFactory final : public FieldReaderFactory {
 public:
  StringFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type)
      : FieldReaderFactory{pool, std::move(veloxType), type} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return createReaderImpl<StringFieldReader>(
        decoders, nimbleType_->asScalar().scalarDescriptor(), wrap(buffer_));
  }

 private:
  std::vector<std::string_view> buffer_;
};

class MultiValueFieldReader : public FieldReader {
 public:
  using FieldReader::FieldReader;

 protected:
  template <typename T, typename... Args>
  velox::vector_size_t loadOffsets(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap,
      velox::vector_size_t allocationSize = 0,
      Args&&... args) {
    auto rowCount = scatterCount(count, scatterBitmap);
    if (allocationSize == 0) {
      allocationSize = rowCount;
    }
    NIMBLE_ASSERT(
        allocationSize >= rowCount,
        fmt::format(
            "readCount should be less than allocationSize. {} vs {}",
            allocationSize,
            rowCount));

    auto vector = VectorInitializer<T>::initialize(
        &pool_, output, type_, allocationSize, std::forward<Args>(args)...);
    vector->resize(allocationSize);

    velox::vector_size_t* sizes =
        vector->mutableSizes(allocationSize)
            ->template asMutable<velox::vector_size_t>();
    velox::vector_size_t* offsets =
        vector->mutableOffsets(allocationSize)
            ->template asMutable<velox::vector_size_t>();

    auto nonNullCount = decoder_->next(
        count,
        sizes,
        [&]() { return paddedNulls(vector, allocationSize); },
        scatterBitmap);

    size_t childrenRows = 0;
    if (nonNullCount == rowCount) {
      vector->resetNulls();
      for (uint32_t i = 0; i < rowCount; ++i) {
        offsets[i] = static_cast<velox::vector_size_t>(childrenRows);
        childrenRows += sizes[i];
      }
    } else {
      vector->setNullCount(rowCount - nonNullCount);

      auto notNulls = reinterpret_cast<const char*>(vector->rawNulls());
      for (uint32_t i = 0; i < rowCount; ++i) {
        offsets[i] = static_cast<velox::vector_size_t>(childrenRows);
        if (bits::getBit(i, notNulls)) {
          childrenRows += sizes[i];
        } else {
          sizes[i] = 0;
        }
      }
    }

    NIMBLE_CHECK(
        childrenRows <= std::numeric_limits<velox::vector_size_t>::max(),
        fmt::format("Unsupported children count: {}", childrenRows));
    return static_cast<velox::vector_size_t>(childrenRows);
  }

  uint32_t skipLengths(uint32_t count) {
    size_t childrenCount = 0;
    std::array<int32_t, kSkipBatchSize> sizes;

    constexpr auto byteSize = nullBytes(kSkipBatchSize);
    std::array<char, byteSize> nulls;

    while (count > 0) {
      auto readSize = std::min(count, kSkipBatchSize);
      auto nonNullCount = decoder_->next(
          readSize,
          sizes.data(),
          [&]() { return nulls.data(); },
          /* scatterBitmap */ nullptr);

      if (nonNullCount == readSize) {
        for (uint32_t i = 0; i < readSize; ++i) {
          childrenCount += sizes[i];
        }
      } else {
        for (uint32_t i = 0; i < readSize; ++i) {
          if (bits::getBit(i, nulls.data())) {
            childrenCount += sizes[i];
          }
        }
      }
      count -= readSize;
    }

    NIMBLE_CHECK(
        childrenCount <= std::numeric_limits<uint32_t>::max(),
        fmt::format("Unsupported children count: {}", childrenCount));
    return static_cast<uint32_t>(childrenCount);
  }
};

class ArrayFieldReader final : public MultiValueFieldReader {
 public:
  ArrayFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::unique_ptr<FieldReader> elementsReader)
      : MultiValueFieldReader{pool, std::move(type), decoder},
        elementsReader_{std::move(elementsReader)} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    const auto* encoding = decoder_->encoding();
    NIMBLE_CHECK(
        encoding != nullptr,
        "Decoder must be loaded for output size estimation.");
    const auto rowCount = encoding->rowCount();

    // Adding memory for velox::BaseVector::nulls_.
    // NOTE: We are not traversing encoding to get the number of nulls as it is
    // expensive for an estimation. We try to be conservative and assume it is
    // nullable.
    totalBytes += rowCount / 8;

    // Adding memory for velox::ArrayVectorBase::sizes_ and
    // velox::ArrayVectorBase::offsets_
    totalBytes += rowCount * sizeof(int32_t) * 2;

    auto rowSize = elementsReader_->estimatedRowSize();
    if (!rowSize.has_value()) {
      return std::nullopt;
    } else {
      const auto elementCount = rowSize.value().first;
      const auto elementAvgSize = rowSize.value().second;
      totalBytes += elementCount * elementAvgSize;
      return rowCount == 0
          ? std::optional<std::pair<uint32_t, uint64_t>>({0, 0})
          : std::optional<std::pair<uint32_t, uint64_t>>(
                {rowCount, totalBytes / rowCount});
    }
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    auto childrenRows = this->template loadOffsets<velox::ArrayVector>(
        count, output, scatterBitmap);

    // As the fields are aligned by lengths decoder so no need to pass scatter
    // to elements
    elementsReader_->next(
        childrenRows,
        static_cast<velox::ArrayVector&>(*output).elements(),
        /* scatterBitmap */ nullptr);
  }

  void skip(uint32_t count) final {
    auto childrenCount = this->skipLengths(count);
    if (childrenCount > 0) {
      elementsReader_->skip(childrenCount);
    }
  }

  void reset() final {
    FieldReader::reset();
    elementsReader_->reset();
  }

 private:
  std::unique_ptr<FieldReader> elementsReader_;
};

class ArrayFieldReaderFactory final : public FieldReaderFactory {
 public:
  // Here the index is the index of the array lengths.
  ArrayFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::unique_ptr<FieldReaderFactory> elements)
      : FieldReaderFactory{pool, std::move(veloxType), type},
        elements_{std::move(elements)} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return createReaderImpl<ArrayFieldReader>(
        decoders, nimbleType_->asArray().lengthsDescriptor(), [&]() {
          return elements_->createReader(decoders);
        });
  }

 private:
  std::unique_ptr<FieldReaderFactory> elements_;
};

class ArrayWithOffsetsFieldReader final : public MultiValueFieldReader {
 public:
  using OffsetType = uint32_t;

  ArrayWithOffsetsFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      Decoder* offsetDecoder,
      std::unique_ptr<FieldReader> elementsReader)
      : MultiValueFieldReader{pool, std::move(type), decoder},
        offsetDecoder_{offsetDecoder},
        elementsReader_{std::move(elementsReader)},
        cached_{false},
        cachedValue_{nullptr},
        cachedIndex_{0},
        cachedSize_{0},
        cachedLazyLoad_{false},
        cachedLazyChildrenRows_{0} {
    VectorInitializer<velox::ArrayVector>::initialize(
        &pool_, cachedValue_, type_, 1);
  }

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    // TODO: Implement estimatedTotalOutputSize for ArrayWithOffsetsFieldReader.
    return std::nullopt;
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    auto rowCount = scatterCount(count, scatterBitmap);
    // read the offsets/indices which is one value per rowCount
    // and filter out deduped arrays to be read
    uint32_t nonNullCount;

    auto dictionaryVector =
        verifyVectorState<velox::DictionaryVector<velox::ComplexType>>(output);

    if (dictionaryVector) {
      dictionaryVector->resize(rowCount);
      resetIfNotWritable(output, dictionaryVector->indices());
    } else {
      velox::VectorPtr child;
      VectorInitializer<velox::ArrayVector>::initialize(
          &pool_, child, type_, rowCount);
      auto indices =
          velox::AlignedBuffer::allocate<OffsetType>(rowCount, &pool_);

      // Note: when creating a dictionary vector, it validates the vector (in
      // debug builds) for correctness. Therefore, we allocate all the buffers
      // above with the right size, but we "resize" them to zero, before
      // creating the dictionary vector, to avoid failing this validation.
      // We will later resize the vector to the correct size.
      // These resize operations are "no cost" operations, as shrinking a
      // vector/buffer doesn't free its memory, and resizing to the original
      // size doesn't allocate, as capacity is guaranteed to be enough.
      child->resize(0);
      indices->setSize(0);

      output = velox::BaseVector::wrapInDictionary(
          /* nulls */ nullptr,
          /* indices */ std::move(indices),
          /* size */ 0,
          /* values */ std::move(child));
      dictionaryVector =
          output->as<velox::DictionaryVector<velox::ComplexType>>();
      dictionaryVector->resize(rowCount);
    }

    void* nullsPtr = nullptr;
    uint32_t dedupCount = getIndicesDeduplicated(
        dictionaryVector->indices()->asMutable<OffsetType>(),
        [&]() {
          // The pointer will be initialized ONLY if the data is nullable.
          // Otherwise, it will remain nullptr, and this is handled below.
          nullsPtr = paddedNulls(dictionaryVector, rowCount);
          return nullsPtr;
        },
        nonNullCount,
        count,
        scatterBitmap);

    bool hasNulls = nonNullCount != rowCount;
    auto indices = dictionaryVector->indices()->asMutable<OffsetType>();
    NIMBLE_DASSERT(indices, "Indices missing.");

    // Returns the first non-null index or -1 (if all are null).
    auto baseIndex = findFirstBit(rowCount, hasNulls, nullsPtr, indices);

    bool cachedLocally = rowCount > 0 && cached_ && (baseIndex == cachedIndex_);

    // Initializes sizes and offsets in the vector.
    auto childrenRows = loadOffsets<velox::ArrayVector>(
        dedupCount - cachedLocally,
        dictionaryVector->valueVector(),
        /* scatterBitmap */ nullptr,
        dedupCount);

    if (cached_ && cachedLazyLoad_) {
      if (cachedLocally) {
        elementsReader_->next(
            cachedLazyChildrenRows_,
            static_cast<velox::ArrayVector&>(*cachedValue_).elements(),
            /* scatterBitmap */ nullptr);
      } else {
        elementsReader_->skip(cachedLazyChildrenRows_);
      }
      cachedLazyLoad_ = false;
    }

    elementsReader_->next(
        childrenRows,
        static_cast<velox::ArrayVector&>(*dictionaryVector->valueVector())
            .elements(),
        /* scatterBitmap */ nullptr);

    if (cachedLocally) {
      auto vector = static_cast<velox::ArrayVector*>(
          dictionaryVector->valueVector().get());

      // Copy elements from cache
      const auto cacheIdx = static_cast<int64_t>(dedupCount) - 1;
      velox::BaseVector::CopyRange cacheRange{
          0, static_cast<velox::vector_size_t>(cacheIdx), 1};
      vector->copyRanges(cachedValue_.get(), folly::Range(&cacheRange, 1));

      // copyRanges overwrites offsets from the source array and must be reset
      OffsetType* sizes =
          vector->mutableSizes(dedupCount)->template asMutable<OffsetType>();
      OffsetType* offsets =
          vector->mutableOffsets(dedupCount)->template asMutable<OffsetType>();

      size_t rows = 0;
      if (cacheIdx > 0) {
        rows = offsets[cacheIdx - 1] + sizes[cacheIdx - 1];
      }

      sizes[cacheIdx] = cachedSize_;
      offsets[cacheIdx] = static_cast<OffsetType>(rows);

      if (hasNulls) {
        vector->setNull(cacheIdx, false);
      }
    }

    // Cache last item
    if (dedupCount > 0) {
      const auto& values = dictionaryVector->valueVector();
      auto idxToCache = std::max(
          0, static_cast<velox::vector_size_t>(dedupCount - 1 - cachedLocally));
      velox::BaseVector::CopyRange cacheRange{
          static_cast<velox::vector_size_t>(idxToCache), 0, 1};

      cachedValue_->prepareForReuse();
      cachedValue_->copyRanges(values.get(), folly::Range(&cacheRange, 1));

      // Get the index for this last element which must be non-null
      cachedIndex_ = indices[findLastBit(rowCount, hasNulls, nullsPtr)];

      cachedSize_ =
          static_cast<velox::ArrayVector&>(*values).sizeAt(idxToCache);
      cached_ = true;
      cachedLazyLoad_ = false;
    }

    // normalize the indices if not all null
    if (nonNullCount > 0) {
      if (hasNulls) {
        NIMBLE_DASSERT(nullsPtr, "Nulls buffer missing.");
        for (OffsetType idx = 0; idx < rowCount; idx++) {
          if (velox::bits::isBitNull(
                  static_cast<const uint64_t*>(nullsPtr), idx)) {
            continue;
          }

          indices[idx] = indices[idx] - baseIndex;
        }
      } else {
        for (OffsetType idx = 0; idx < rowCount; idx++) {
          indices[idx] = indices[idx] - baseIndex;
        }
      }
    }

    // update the indices as per cached and null locations
    if (hasNulls) {
      dictionaryVector->setNullCount(nonNullCount != rowCount);
      NIMBLE_DASSERT(nullsPtr, "Nulls buffer missing.");
      for (OffsetType idx = 0; idx < rowCount; idx++) {
        if (velox::bits::isBitNull(
                static_cast<const uint64_t*>(nullsPtr), idx)) {
          indices[idx] = dedupCount - 1;
        } else {
          if (indices[idx] == 0 && cachedLocally) { // cached index
            indices[idx] = dedupCount - 1;
          } else {
            indices[idx] -= cachedLocally;
          }
        }
      }
    } else {
      dictionaryVector->resetNulls();
      for (OffsetType idx = 0; idx < rowCount; idx++) {
        if (indices[idx] == 0 && cachedLocally) { // cached index
          indices[idx] = dedupCount - 1;
        } else {
          indices[idx] -= cachedLocally;
        }
      }
    }
  }

  void skip(uint32_t count) final {
    // read the offsets/indices which is one value per rowCount
    // and filter out deduped arrays to be read
    std::array<OffsetType, kSkipBatchSize> indices;
    std::array<char, nullBytes(kSkipBatchSize)> nulls;
    void* nullsPtr = nulls.data();
    uint32_t nonNullCount;

    while (count > 0) {
      auto batchedRowCount = std::min(count, kSkipBatchSize);
      uint32_t dedupCount = getIndicesDeduplicated(
          indices.data(),
          [&]() { return nullsPtr; },
          nonNullCount,
          batchedRowCount);

      bool hasNulls = nonNullCount != batchedRowCount;

      // baseIndex is the first non-null index
      auto baseIndex =
          findFirstBit(batchedRowCount, hasNulls, nullsPtr, indices.data());

      bool cachedLocally = cached_ && (baseIndex == cachedIndex_);
      if (cachedLocally) {
        dedupCount--;
      }

      // skip all the children except the last one
      if (dedupCount > 0) {
        auto childrenRows =
            cached_ && cachedLazyLoad_ ? cachedLazyChildrenRows_ : 0;
        childrenRows += this->skipLengths(dedupCount - 1);
        if (childrenRows > 0) {
          elementsReader_->skip(childrenRows);
        }

        /// cache the last child

        // get the index for this last element which must be non-null
        cachedIndex_ =
            indices[findLastBit(batchedRowCount, hasNulls, nullsPtr)];
        cached_ = true;
        cachedLazyLoad_ = true;
        cachedLazyChildrenRows_ =
            loadOffsets<velox::ArrayVector>(1, cachedValue_, nullptr);

        cachedSize_ = static_cast<velox::ArrayVector&>(*cachedValue_).sizeAt(0);
      }

      count -= batchedRowCount;
    }
  }

  void reset() final {
    FieldReader::reset();
    offsetDecoder_->reset();
    cached_ = false;
    elementsReader_->reset();
  }

 private:
  Decoder* offsetDecoder_;
  std::unique_ptr<FieldReader> elementsReader_;
  bool cached_;
  velox::VectorPtr cachedValue_;
  OffsetType cachedIndex_;
  uint32_t cachedSize_;
  bool cachedLazyLoad_;
  uint32_t cachedLazyChildrenRows_;

  static inline OffsetType findLastBit(
      uint32_t rowCount,
      bool hasNulls,
      const void* FOLLY_NULLABLE nulls) {
    if (!hasNulls) {
      return rowCount - 1;
    }

    NIMBLE_DASSERT(nulls, "Nulls buffer missing.");
    auto index = velox::bits::findLastBit(
        static_cast<const uint64_t*>(nulls), 0, rowCount);
    if (index == -1) {
      return rowCount - 1;
    }

    return index;
  }

  static inline int32_t findFirstBit(
      uint32_t rowCount,
      bool hasNulls,
      const void* FOLLY_NULLABLE nulls,
      const OffsetType* indices) {
    if (!hasNulls) {
      return indices[0];
    }

    NIMBLE_DASSERT(nulls, "Nulls buffer missing.");
    auto index = velox::bits::findFirstBit(
        static_cast<const uint64_t*>(nulls), 0, rowCount);

    if (index == -1) {
      return -1;
    }

    return indices[index];
  }

  uint32_t getIndicesDeduplicated(
      OffsetType* indices,
      std::function<void*()> nulls,
      uint32_t& nonNullCount,
      uint32_t count,
      const bits::Bitmap* scatterBitmap = nullptr) {
    auto rowCount = scatterCount(count, scatterBitmap);
    // OffsetType* indices = dictIndices->asMutable<OffsetType>();
    void* nullsPtr;

    nonNullCount = offsetDecoder_->next(
        count,
        indices,
        [&]() {
          nullsPtr = nulls();
          return nullsPtr;
        },
        scatterBitmap);

    // remove duplicated indices and calculate unique count
    uint32_t uniqueCount = 0;
    uint32_t prevIdx = 0;
    bool hasNulls = nonNullCount != rowCount;

    if (hasNulls) {
      NIMBLE_DASSERT(
          nullsPtr != nullptr,
          "Data contain nulls but nulls buffer is not initialized.");

      for (uint32_t idx = 0; idx < rowCount; idx++) {
        if (velox::bits::isBitNull(
                static_cast<const uint64_t*>(nullsPtr), idx)) {
          indices[idx] = 0;
          continue;
        }

        if (uniqueCount == 0 || indices[idx] != indices[prevIdx]) {
          uniqueCount++;
        }
        prevIdx = idx;
      }
    } else {
      for (uint32_t idx = 0; idx < rowCount; idx++) {
        if (uniqueCount == 0 || indices[idx] != indices[prevIdx]) {
          uniqueCount++;
        }
        prevIdx = idx;
      }
    }

    return uniqueCount;
  }
};

class ArrayWithOffsetsFieldReaderFactory final : public FieldReaderFactory {
 public:
  // Here the index is the index of the array lengths.
  ArrayWithOffsetsFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::unique_ptr<FieldReaderFactory> elements)
      : FieldReaderFactory{pool, std::move(veloxType), type},
        elements_{std::move(elements)} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return createReaderImpl<ArrayWithOffsetsFieldReader>(
        decoders,
        nimbleType_->asArrayWithOffsets().lengthsDescriptor(),
        [&]() {
          return getDecoder(
              decoders, nimbleType_->asArrayWithOffsets().offsetsDescriptor());
        },
        [&]() { return elements_->createReader(decoders); });
  }

 private:
  std::unique_ptr<FieldReaderFactory> elements_;
};

class SlidingWindowMapFieldReader final : public FieldReader {
 public:
  SlidingWindowMapFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* offsetDecoder,
      Decoder* lengthsDecoder,
      std::unique_ptr<FieldReader> keysReader,
      std::unique_ptr<FieldReader> valuesReader)
      : FieldReader(pool, std::move(type), nullptr),
        offsetDecoder_{offsetDecoder},
        lengthsDecoder_{lengthsDecoder},
        keysReader_{std::move(keysReader)},
        valuesReader_{std::move(valuesReader)} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    // TODO: Implement estimatedTotalOutputSize for SlidingWindowMapFieldReader.
    return std::nullopt;
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) override {
    auto rowCount = scatterCount(count, scatterBitmap);

    auto dictionaryVector =
        verifyVectorState<velox::DictionaryVector<velox::ComplexType>>(output);

    // Initialize the output vector
    if (dictionaryVector) {
      dictionaryVector->resize(rowCount);
      dictionaryVector->resetNulls();
      resetIfNotWritable(output, dictionaryVector->indices());
      auto child =
          verifyVectorState<velox::MapVector>(dictionaryVector->valueVector());
      if (child) {
        child->resize(rowCount);
      } else {
        VectorInitializer<velox::MapVector>::initialize(
            &pool_, dictionaryVector->valueVector(), type_, rowCount);
      }
    } else {
      velox::VectorPtr child;
      VectorInitializer<velox::MapVector>::initialize(
          &pool_, child, type_, rowCount);
      auto indices = velox::AlignedBuffer::allocate<uint32_t>(rowCount, &pool_);

      // Note: when creating a dictionary vector, it validates the vector (in
      // debug builds) for correctness. Therefore, we allocate all the buffers
      // above with the right size, but we "resize" them to zero, before
      // creating the dictionary vector, to avoid failing this validation.
      // We will later resize the vector to the correct size.
      // These resize operations are "no cost" operations, as shrinking a
      // vector/buffer doesn't free its memory, and resizing to the original
      // size doesn't allocate, as capacity is guaranteed to be enough.
      child->resize(0);
      indices->setSize(0);

      output = velox::BaseVector::wrapInDictionary(
          /* nulls */ nullptr,
          /* indices */ std::move(indices),
          /* size */ 0,
          /* values */ std::move(child));
      dictionaryVector =
          output->as<velox::DictionaryVector<velox::ComplexType>>();
      dictionaryVector->resize(rowCount);
    }

    // Read the offsets which can be nullable
    auto indices = dictionaryVector->indices()->asMutable<uint32_t>();
    void* nullsPtr = nullptr;
    uint32_t nonNullCount = offsetDecoder_->next(
        count,
        indices,
        [&]() {
          nullsPtr = paddedNulls(dictionaryVector, rowCount);
          return nullsPtr;
        },
        scatterBitmap);

    // Return early if everything is null
    if (nonNullCount == 0) {
      return;
    }

    bool hasNulls = nonNullCount != rowCount;
    // Read the lengths
    uint32_t lengthBuffer[nonNullCount];
    lengthsDecoder_->next(nonNullCount, lengthBuffer);

    // Convert the offsets and lengths to a list of unique offsets and lengths
    // and update the indices to be 0-based indices
    std::vector<uint32_t> deduplicatedOffsets, deduplicatedLengths;
    deduplicatedOffsets.reserve(nonNullCount);
    deduplicatedLengths.reserve(nonNullCount);
    uint32_t uniqueCount = 0, startOffset = 0, endOffset = 0;
    if (hasNulls) {
      NIMBLE_DASSERT(
          nullsPtr != nullptr,
          "Data contain nulls but nulls buffer is not initialized.");
      uint32_t nullCount = 0;
      for (uint32_t idx = 0; idx < rowCount; ++idx) {
        if (velox::bits::isBitNull(
                static_cast<const uint64_t*>(nullsPtr), idx)) {
          indices[idx] = 0;
          ++nullCount;
          continue;
        }
        // Firt non-null item
        if (deduplicatedOffsets.empty()) {
          deduplicatedOffsets.push_back(indices[idx]);
          deduplicatedLengths.push_back(lengthBuffer[idx - nullCount]);
          startOffset = deduplicatedOffsets.back();
          endOffset = deduplicatedOffsets.back() + deduplicatedLengths.back();
          uniqueCount = 1;
        } else if (
            // Check if the current item is the same as the last one
            // If not, update deduplicatedOffsets and deduplicatedLengths
            deduplicatedOffsets.back() != indices[idx] ||
            deduplicatedLengths.back() != lengthBuffer[idx - nullCount]) {
          deduplicatedOffsets.push_back(indices[idx]);
          deduplicatedLengths.push_back(lengthBuffer[idx - nullCount]);
          endOffset = std::max(
              deduplicatedOffsets.back() + deduplicatedLengths.back(),
              endOffset);
          ++uniqueCount;
        }
        indices[idx] = uniqueCount - 1;
      }
      NIMBLE_ASSERT(
          nonNullCount + nullCount == rowCount, "Null Count is not matching");
    } else {
      deduplicatedOffsets.push_back(indices[0]);
      deduplicatedLengths.push_back(lengthBuffer[0]);
      startOffset = deduplicatedOffsets.back();
      endOffset = deduplicatedOffsets.back() + deduplicatedLengths.back();
      indices[0] = 0;
      ++uniqueCount;
      // Start from the second item, check if the current item is the same as
      // the last one If not, update deduplicatedOffsets and deduplicatedLengths
      for (uint32_t idx = 1; idx < rowCount; ++idx) {
        if (deduplicatedOffsets.back() != indices[idx] ||
            deduplicatedLengths.back() != lengthBuffer[idx]) {
          deduplicatedOffsets.push_back(indices[idx]);
          deduplicatedLengths.push_back(lengthBuffer[idx]);
          endOffset = std::max(
              deduplicatedOffsets.back() + deduplicatedLengths.back(),
              endOffset);
          ++uniqueCount;
        }
        indices[idx] = uniqueCount - 1;
      }
    }

    // Fill the map vector
    auto map =
        static_cast<velox::MapVector*>(dictionaryVector->valueVector().get());
    map->resize(uniqueCount);
    auto offsets =
        map->mutableOffsets(uniqueCount)->template asMutable<uint32_t>();
    auto sizes = map->mutableSizes(uniqueCount)->template asMutable<uint32_t>();

    uint32_t mapOffset = 0;
    for (uint32_t i = 0; i < uniqueCount; ++i) {
      sizes[i] = deduplicatedLengths[i];
      offsets[i] = mapOffset;
      mapOffset += deduplicatedLengths[i];
    }

    uint32_t childrenRows = endOffset - startOffset;
    if (childrenRows > 0) {
      seek(startOffset);
      // Read the keys and values
      keysReader_->next(
          childrenRows,
          map->mapKeys(),
          /* scatterBitmap */ nullptr);
      valuesReader_->next(
          childrenRows,
          map->mapValues(),
          /* scatterBitmap */ nullptr);
      cursorPos_ = endOffset;
    }
  }

  void skip(uint32_t count) final {
    std::array<uint32_t, kSkipBatchSize> offsets;
    std::array<char, nullBytes(kSkipBatchSize)> nulls;
    void* nullsPtr = nulls.data();

    while (count > 0) {
      auto skipSize = std::min(count, kSkipBatchSize);
      auto nonNullCount = offsetDecoder_->next(
          skipSize,
          offsets.data(),
          [&]() { return nullsPtr; },
          /* scatterBitmap */ nullptr);
      lengthsDecoder_->skip(nonNullCount);
      count -= skipSize;
    }
  }

  // Move the cursor of key and value readers to the given offset
  void seek(uint32_t offset) {
    if (offset == cursorPos_) {
      return;
    } else if (offset < cursorPos_) {
      keysReader_->reset();
      valuesReader_->reset();
      keysReader_->skip(offset);
      valuesReader_->skip(offset);
    } else {
      keysReader_->skip(offset - cursorPos_);
      valuesReader_->skip(offset - cursorPos_);
    }
    cursorPos_ = offset;
  }

  void reset() final {
    FieldReader::reset();
    offsetDecoder_->reset();
    lengthsDecoder_->reset();
    keysReader_->reset();
    valuesReader_->reset();
  }

 private:
  Decoder* offsetDecoder_;
  Decoder* lengthsDecoder_;
  std::unique_ptr<FieldReader> keysReader_;
  std::unique_ptr<FieldReader> valuesReader_;
  uint32_t cursorPos_{0};
};

class SlidingWindowMapFieldReaderFactory final : public FieldReaderFactory {
 public:
  // Here the index is the index of the array lengths.
  SlidingWindowMapFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::unique_ptr<FieldReaderFactory> keys,
      std::unique_ptr<FieldReaderFactory> values)
      : FieldReaderFactory{pool, std::move(veloxType), type},
        keys_{std::move(keys)},
        values_{std::move(values)} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return createReaderImpl<SlidingWindowMapFieldReader>(
        decoders,
        nimbleType_->asSlidingWindowMap().offsetsDescriptor(),
        [&]() {
          return getDecoder(
              decoders, nimbleType_->asSlidingWindowMap().lengthsDescriptor());
        },
        [&]() { return keys_->createReader(decoders); },
        [&]() { return values_->createReader(decoders); });
  }

 private:
  std::unique_ptr<FieldReaderFactory> keys_;
  std::unique_ptr<FieldReaderFactory> values_;
};

class MapFieldReader final : public MultiValueFieldReader {
 public:
  MapFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::unique_ptr<FieldReader> keysReader,
      std::unique_ptr<FieldReader> valuesReader)
      : MultiValueFieldReader{pool, std::move(type), decoder},
        keysReader_{std::move(keysReader)},
        valuesReader_{std::move(valuesReader)} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    const auto* encoding = decoder_->encoding();
    NIMBLE_CHECK(
        encoding != nullptr,
        "Decoder must be loaded for output size estimation.");
    const auto rowCount = encoding->rowCount();

    // Adding memory for velox::BaseVector::nulls_.
    // NOTE: We are not traversing encoding to get the number of nulls as it is
    // expensive for an estimation. We try to be conservative and assume it is
    // nullable.
    totalBytes += rowCount / 8;

    // Adding memory for velox::MapVector::sizes_ and velox::MapVector::offsets_
    totalBytes += rowCount * sizeof(int32_t) * 2;

    auto keySize = keysReader_->estimatedRowSize();
    if (!keySize.has_value()) {
      return std::nullopt;
    }
    auto valueSize = valuesReader_->estimatedRowSize();
    if (!valueSize.has_value()) {
      return std::nullopt;
    }
    totalBytes += keySize.value().first * keySize.value().second +
        valueSize.value().first * valueSize.value().second;
    return rowCount == 0 ? std::optional<std::pair<uint32_t, uint64_t>>({0, 0})
                         : std::optional<std::pair<uint32_t, uint64_t>>(
                               {rowCount, totalBytes / rowCount});
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    auto childrenRows = this->template loadOffsets<velox::MapVector>(
        count, output, scatterBitmap);

    // As the field is aligned by lengths decoder then no need to pass
    // scatterBitmap to keys and values
    auto& mapVector = static_cast<velox::MapVector&>(*output);
    keysReader_->next(
        childrenRows, mapVector.mapKeys(), /* scatterBitmap */ nullptr);
    valuesReader_->next(
        childrenRows, mapVector.mapValues(), /* scatterBitmap */ nullptr);
  }

  void skip(uint32_t count) final {
    auto childrenCount = this->skipLengths(count);
    if (childrenCount > 0) {
      keysReader_->skip(childrenCount);
      valuesReader_->skip(childrenCount);
    }
  }

  void reset() final {
    FieldReader::reset();
    keysReader_->reset();
    valuesReader_->reset();
  }

 private:
  std::unique_ptr<FieldReader> keysReader_;
  std::unique_ptr<FieldReader> valuesReader_;
};

class MapFieldReaderFactory final : public FieldReaderFactory {
 public:
  // Here the index is the index of the array lengths.
  MapFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::unique_ptr<FieldReaderFactory> keys,
      std::unique_ptr<FieldReaderFactory> values)
      : FieldReaderFactory{pool, std::move(veloxType), type},
        keys_{std::move(keys)},
        values_{std::move(values)} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return createReaderImpl<MapFieldReader>(
        decoders,
        nimbleType_->asMap().lengthsDescriptor(),
        [&]() { return keys_->createReader(decoders); },
        [&]() { return values_->createReader(decoders); });
  }

 private:
  std::unique_ptr<FieldReaderFactory> keys_;
  std::unique_ptr<FieldReaderFactory> values_;
};

// Read values from boolean decoder and return number of true values.
template <typename TrueHandler>
uint32_t readBooleanValues(
    Decoder* decoder,
    bool* buffer,
    uint32_t count,
    TrueHandler handler) {
  decoder->next(count, buffer);

  uint32_t trueCount = 0;
  for (uint32_t i = 0; i < count; ++i) {
    if (buffer[i]) {
      handler(i);
      ++trueCount;
    }
  }
  return trueCount;
}

uint32_t readBooleanValues(Decoder* decoder, bool* buffer, uint32_t count) {
  return readBooleanValues(decoder, buffer, count, [](auto /* ignored */) {});
}

namespace {
// Per row overhead on velox vector for null value. Returns overhead in bits.
uint64_t nullOverheadBits(const velox::TypePtr& type) {
  switch (type->kind()) {
    case velox::TypeKind::BOOLEAN:
      return 1;
    case velox::TypeKind::TINYINT:
      return 8 *
          sizeof(velox::TypeTraits<velox::TypeKind::TINYINT>::NativeType);
    case velox::TypeKind::SMALLINT:
      return 8 *
          sizeof(velox::TypeTraits<velox::TypeKind::SMALLINT>::NativeType);
    case velox::TypeKind::INTEGER:
      return 8 *
          sizeof(velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType);
    case velox::TypeKind::BIGINT:
      return 8 * sizeof(velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType);
    case velox::TypeKind::REAL:
      return 8 * sizeof(velox::TypeTraits<velox::TypeKind::REAL>::NativeType);
    case velox::TypeKind::DOUBLE:
      return 8 * sizeof(velox::TypeTraits<velox::TypeKind::DOUBLE>::NativeType);
    case velox::TypeKind::HUGEINT:
      return 8 *
          sizeof(velox::TypeTraits<velox::TypeKind::HUGEINT>::NativeType);
    case velox::TypeKind::TIMESTAMP:
      return 8 *
          sizeof(velox::TypeTraits<velox::TypeKind::TIMESTAMP>::NativeType);
    case velox::TypeKind::UNKNOWN:
      return 8 *
          sizeof(velox::TypeTraits<velox::TypeKind::UNKNOWN>::NativeType);
    case velox::TypeKind::VARCHAR:
      [[fallthrough]];
    case velox::TypeKind::VARBINARY:
      return 8 * sizeof(velox::StringView);
    case velox::TypeKind::ARRAY:
      [[fallthrough]];
    case velox::TypeKind::MAP:
      // 4 bytes per row on sizes_ and 4 bytes per row on offsets_
      return 8 * 8;
    case velox::TypeKind::ROW:
      [[fallthrough]];
    default:
      // Not adding nulls overhead (reduced accuracy) for unknown types.
      return 0;
  }
}
} // namespace

template <bool hasNull>
class RowFieldReader final : public FieldReader {
 public:
  RowFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::vector<std::unique_ptr<FieldReader>> childrenReaders,
      Vector<bool>& boolBuffer,
      folly::Executor* executor)
      : FieldReader{pool, std::move(type), decoder},
        childrenReaders_{std::move(childrenReaders)},
        boolBuffer_{boolBuffer},
        executor_{executor} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    uint64_t rowCount{0};
    if constexpr (hasNull) {
      const auto* encoding = decoder_->encoding();
      NIMBLE_CHECK(
          encoding != nullptr,
          "Decoder must be loaded for output size estimation.");
      rowCount = encoding->rowCount();
      // Adding memory for velox::BaseVector::nulls_
      totalBytes += rowCount / 8;
    }

    for (auto& reader : childrenReaders_) {
      // Non selected fields are set to null.
      if (reader == nullptr) {
        continue;
      }
      auto childSize = reader->estimatedRowSize();
      if (!childSize.has_value()) {
        return std::nullopt;
      }

      // Add non-null size
      const auto childRowCount = childSize.value().first;
      totalBytes += childRowCount * childSize.value().second;

      // Add null size
      if constexpr (hasNull) {
        const auto nullCount = rowCount - childSize.value().first;
        totalBytes += nullCount * nullOverheadBits(reader->type()) / 8;
      } else if (rowCount == 0) {
        rowCount = childRowCount;
      } else {
        NIMBLE_CHECK(
            rowCount == childRowCount,
            fmt::format(
                "rowCount {} should be equal to childRowCount {} under no null "
                "condition.",
                rowCount,
                childRowCount));
      }
    }

    return rowCount == 0 ? std::optional<std::pair<uint32_t, uint64_t>>({0, 0})
                         : std::optional<std::pair<uint32_t, uint64_t>>(
                               {rowCount, totalBytes / rowCount});
  }

  void next(
      uint32_t count,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    auto rowCount = scatterCount(count, scatterBitmap);
    auto vector = VectorInitializer<velox::RowVector>::initialize(
        &pool_, output, type_, rowCount);
    vector->children().resize(childrenReaders_.size());
    vector->unsafeResize(rowCount);
    const void* childrenBits = nullptr;
    uint32_t selectedNonNullCount = 0;

    if constexpr (hasNull) {
      zeroNulls(vector, rowCount);
      // if it is a scattered read case then we can't read the rowCount
      // values from the nulls, we count the set value in scatterBitmap and
      // read only those values, if there is no scatter then we can read
      // rowCount values
      boolBuffer_.resize(count);
      decoder_->next(count, boolBuffer_.data());

      auto* nullBuffer = paddedNulls(vector, rowCount);
      bits::BitmapBuilder nullBits{nullBuffer, rowCount};
      if (scatterBitmap) {
        uint32_t boolBufferOffset = 0;
        for (uint32_t i = 0; i < rowCount; ++i) {
          if (scatterBitmap->test(i) && boolBuffer_[boolBufferOffset++]) {
            nullBits.set(i);
            ++selectedNonNullCount;
          }
        }
      } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
          if (boolBuffer_[i]) {
            nullBits.set(i);
            ++selectedNonNullCount;
          }
        }
      }
      if (UNLIKELY(selectedNonNullCount == rowCount)) {
        vector->resetNulls();
      } else {
        vector->setNullCount(rowCount - selectedNonNullCount);
        childrenBits = nullBuffer;
      }
    } else {
      selectedNonNullCount = count;
      if (scatterBitmap) {
        auto requiredBytes = bits::bytesRequired(rowCount);
        auto* nullBuffer = paddedNulls(vector, rowCount);
        // @lint-ignore CLANGSECURITY facebook-security-vulnerable-memcpy
        std::memcpy(
            nullBuffer,
            static_cast<const char*>(scatterBitmap->bits()),
            requiredBytes);
        vector->setNullCount(rowCount - count);
        childrenBits = scatterBitmap->bits();
      } else {
        vector->resetNulls();
      }
    }

    if (executor_) {
      for (uint32_t i = 0; i < childrenReaders_.size(); ++i) {
        auto& reader = childrenReaders_[i];
        if (reader) {
          executor_->add([selectedNonNullCount,
                          childrenBits,
                          rowCount,
                          &reader,
                          &child = vector->childAt(i)]() {
            bits::Bitmap childrenBitmap{childrenBits, rowCount};
            auto bitmapPtr = childrenBits ? &childrenBitmap : nullptr;
            reader->next(selectedNonNullCount, child, bitmapPtr);
          });
        }
      }
    } else {
      bits::Bitmap childrenBitmap{childrenBits, rowCount};
      auto bitmapPtr = childrenBits ? &childrenBitmap : nullptr;
      for (uint32_t i = 0; i < childrenReaders_.size(); ++i) {
        auto& reader = childrenReaders_[i];
        if (reader) {
          reader->next(selectedNonNullCount, vector->childAt(i), bitmapPtr);
        }
      }
    }
  }

  void skip(uint32_t count) final {
    uint32_t childRowCount = count;
    if constexpr (hasNull) {
      std::array<bool, kSkipBatchSize> buffer;
      childRowCount = 0;
      while (count > 0) {
        auto readSize = std::min(count, kSkipBatchSize);
        childRowCount += readBooleanValues(decoder_, buffer.data(), readSize);
        count -= readSize;
      }
    }

    if (childRowCount > 0) {
      for (auto& reader : childrenReaders_) {
        if (reader) {
          reader->skip(childRowCount);
        }
      }
    }
  }

  void reset() final {
    FieldReader::reset();
    for (auto& reader : childrenReaders_) {
      if (reader) {
        reader->reset();
      }
    }
  }

 private:
  std::vector<std::unique_ptr<FieldReader>> childrenReaders_;
  Vector<bool>& boolBuffer_;
  folly::Executor* executor_;
};

class RowFieldReaderFactory final : public FieldReaderFactory {
 public:
  // Here the index is the index of the null decoder.
  RowFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::vector<std::unique_ptr<FieldReaderFactory>> children,
      folly::Executor* executor)
      : FieldReaderFactory{pool, std::move(veloxType), type},
        children_{std::move(children)},
        boolBuffer_{&pool_},
        executor_{executor} {}

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    auto nulls = getDecoder(decoders, nimbleType_->asRow().nullsDescriptor());

    std::vector<std::unique_ptr<FieldReader>> childrenReaders(children_.size());
    for (uint32_t i = 0; i < children_.size(); ++i) {
      auto& child = children_[i];
      if (child) {
        // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
        childrenReaders[i] = child->createReader(decoders);
      }
    }

    if (!nulls) {
      return std::make_unique<RowFieldReader<false>>(
          pool_,
          veloxType_,
          nulls,
          std::move(childrenReaders),
          boolBuffer_,
          executor_);
    }

    return std::make_unique<RowFieldReader<true>>(
        pool_,
        veloxType_,
        nulls,
        std::move(childrenReaders),
        boolBuffer_,
        executor_);
  }

 private:
  std::vector<std::unique_ptr<FieldReaderFactory>> children_;
  Vector<bool> boolBuffer_;
  folly::Executor* executor_;
};

// Represent a keyed value node for flat map
// Before reading the value, InMap vectors we need to call load()
template <typename T>
class FlatMapKeyNode {
 public:
  FlatMapKeyNode(
      velox::memory::MemoryPool& memoryPool,
      std::unique_ptr<FieldReader> valueReader,
      Decoder* inMapDecoder,
      const velox::dwio::common::flatmap::KeyValue<T>& key)
      : valueReader_{std::move(valueReader)},
        inMapDecoder_{inMapDecoder},
        inMapData_{&memoryPool},
        key_{key},
        mergedNulls_{&memoryPool} {}

  ~FlatMapKeyNode() = default;

  void readAsChild(
      velox::VectorPtr& vector,
      uint32_t numValues,
      uint32_t nonNullValues,
      const Vector<bool>& mapNulls,
      Vector<char>* mergedNulls = nullptr) {
    if (!mergedNulls) {
      mergedNulls = &mergedNulls_;
    }
    auto nonNullCount =
        mergeNulls(numValues, nonNullValues, mapNulls, *mergedNulls);
    bits::Bitmap bitmap{mergedNulls->data(), numValues};
    valueReader_->next(nonNullCount, vector, &bitmap);
    NIMBLE_DCHECK(numValues == vector->size(), "Items not loaded");
  }

  uint32_t readInMapData(uint32_t numValues) {
    inMapData_.resize(numValues);
    numValues_ = readBooleanValues(inMapDecoder_, inMapData_.data(), numValues);
    return numValues_;
  }

  void loadValues(velox::VectorPtr& values) {
    valueReader_->next(numValues_, values, /* scatterBitmap */ nullptr);
    NIMBLE_DCHECK(numValues_ == values->size(), "Items not loaded");
  }

  void skip(uint32_t numValues) {
    auto numItems = readInMapData(numValues);
    if (numItems > 0) {
      valueReader_->skip(numItems);
    }
  }

  const velox::dwio::common::flatmap::KeyValue<T>& key() const {
    return key_;
  }

  const FieldReader* valueReader() const {
    return valueReader_.get();
  }

  const Decoder* inMapDecoder() const {
    return inMapDecoder_;
  }

  bool inMap(uint32_t index) const {
    return inMapData_[index];
  }

  void reset() {
    inMapDecoder_->reset();
    valueReader_->reset();
  }

 private:
  // Merge the mapNulls and inMapData into mergedNulls
  uint32_t mergeNulls(
      uint32_t numValues,
      uint32_t nonNullMaps,
      const Vector<bool>& mapNulls,
      Vector<char>& mergedNulls) {
    const auto numItems = readInMapData(nonNullMaps);
    auto requiredBytes = bits::bytesRequired(numValues);
    mergedNulls.resize(requiredBytes);
    memset(mergedNulls.data(), 0, requiredBytes);
    if (numItems == 0) {
      return 0;
    }

    if (nonNullMaps == numValues) {
      // All values are nonNull
      bits::packBitmap(inMapData_, mergedNulls.data());
      return numItems;
    }
    uint32_t inMapOffset = 0;
    for (uint32_t i = 0; i < numValues; ++i) {
      if (mapNulls[i] && inMapData_[inMapOffset++]) {
        bits::setBit(i, mergedNulls.data());
      }
    }
    return numItems;
  }

  std::unique_ptr<FieldReader> valueReader_;
  Decoder* inMapDecoder_;
  Vector<bool> inMapData_;
  const velox::dwio::common::flatmap::KeyValue<T>& key_;
  uint32_t numValues_;
  // nulls buffer used in parallel read cases.
  Vector<char> mergedNulls_;
};

template <typename T, bool hasNull>
class FlatMapFieldReaderBase : public FieldReader {
 public:
  FlatMapFieldReaderBase(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::vector<std::unique_ptr<FlatMapKeyNode<T>>> keyNodes,
      Vector<bool>& boolBuffer)
      : FieldReader{pool, std::move(type), decoder},
        keyNodes_{std::move(keyNodes)},
        boolBuffer_{boolBuffer} {}

  uint32_t loadNulls(uint32_t rowCount, velox::BaseVector* vector) {
    if constexpr (hasNull) {
      zeroNulls(vector, rowCount);
      auto* nullBuffer = paddedNulls(vector, rowCount);
      bits::BitmapBuilder bitmap{nullBuffer, rowCount};

      boolBuffer_.resize(rowCount);
      auto nonNullCount = readBooleanValues(
          decoder_, boolBuffer_.data(), rowCount, [&](auto i) {
            bitmap.set(i);
          });

      if (UNLIKELY(nonNullCount == rowCount)) {
        vector->resetNulls();
      } else {
        vector->setNullCount(rowCount - nonNullCount);
      }
      return nonNullCount;
    } else {
      vector->resetNulls();
      return rowCount;
    }
  }

  void skip(uint32_t count) final {
    uint32_t nonNullCount = count;

    if constexpr (hasNull) {
      std::array<bool, kSkipBatchSize> buffer;
      nonNullCount = 0;
      while (count > 0) {
        auto readSize = std::min(count, kSkipBatchSize);
        nonNullCount += readBooleanValues(decoder_, buffer.data(), readSize);
        count -= readSize;
      }
    }

    if (nonNullCount > 0) {
      for (auto& node : keyNodes_) {
        if (node) {
          node->skip(nonNullCount);
        }
      }
    }
  }

  void reset() final {
    FieldReader::reset();
    for (auto& node : keyNodes_) {
      if (node) {
        node->reset();
      }
    }
  }

 protected:
  std::vector<std::unique_ptr<FlatMapKeyNode<T>>> keyNodes_;
  Vector<bool>& boolBuffer_;
};

template <typename T>
class FlatMapFieldReaderFactoryBase : public FieldReaderFactory {
 public:
  FlatMapFieldReaderFactoryBase(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::vector<const StreamDescriptor*> inMapDescriptors,
      std::vector<std::unique_ptr<FieldReaderFactory>> valueReaders,
      const std::vector<size_t>& selectedChildren)
      : FieldReaderFactory{pool, std::move(veloxType), type},
        inMapDescriptors_{std::move(inMapDescriptors)},
        valueReaders_{std::move(valueReaders)},
        boolBuffer_{&pool_} {
    // inMapTypes contains all projected children, including those that don't
    // exist in the schema. selectedChildren and valuesReaders only contain
    // those that also exist in the schema.
    NIMBLE_ASSERT(
        inMapDescriptors_.size() >= valueReaders_.size(),
        "Value and inMaps size mismatch!");
    NIMBLE_ASSERT(
        selectedChildren.size() == valueReaders_.size(),
        "Selected children and value readers size mismatch!");

    auto& flatMap = type->asFlatMap();
    keyValues_.reserve(selectedChildren.size());
    for (auto childIdx : selectedChildren) {
      keyValues_.push_back(velox::dwio::common::flatmap::parseKeyValue<T>(
          flatMap.nameAt(childIdx)));
    }
  }

  template <
      template <bool>
      typename ReaderT,
      bool includeMissing,
      typename... Args>
  std::unique_ptr<FieldReader> createFlatMapReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders,
      Args&&... args) {
    auto nulls =
        getDecoder(decoders, nimbleType_->asFlatMap().nullsDescriptor());

    std::vector<std::unique_ptr<FlatMapKeyNode<T>>> keyNodes;
    keyNodes.reserve(valueReaders_.size());
    uint32_t childIdx = 0;
    for (auto inMapDescriptor : inMapDescriptors_) {
      if (inMapDescriptor) {
        auto currentIdx = childIdx++;
        if (auto decoder = getDecoder(decoders, *inMapDescriptor)) {
          keyNodes.push_back(std::make_unique<FlatMapKeyNode<T>>(
              pool_,
              // @lint-ignore CLANGTIDY facebook-hte-MemberUncheckedArrayBounds
              valueReaders_[currentIdx]->createReader(decoders),
              decoder,
              // @lint-ignore CLANGTIDY facebook-hte-MemberUncheckedArrayBounds
              keyValues_[currentIdx]));
          continue;
        }
      }

      if constexpr (includeMissing) {
        keyNodes.push_back(nullptr);
      }
    }

    if (!nulls) {
      return std::make_unique<ReaderT<false>>(
          pool_,
          this->veloxType_,
          nulls,
          std::move(keyNodes),
          boolBuffer_,
          std::forward<Args>(args)...);
    }

    return std::make_unique<ReaderT<true>>(
        pool_,
        this->veloxType_,
        nulls,
        std::move(keyNodes),
        boolBuffer_,
        std::forward<Args>(args)...);
  }

 protected:
  std::vector<const StreamDescriptor*> inMapDescriptors_;
  std::vector<std::unique_ptr<FieldReaderFactory>> valueReaders_;
  std::vector<velox::dwio::common::flatmap::KeyValue<T>> keyValues_;
  Vector<bool> boolBuffer_;
};

template <typename T, bool hasNull>
class StructFlatMapFieldReader : public FlatMapFieldReaderBase<T, hasNull> {
 public:
  StructFlatMapFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::vector<std::unique_ptr<FlatMapKeyNode<T>>> keyNodes,
      Vector<bool>& boolBuffer,
      Vector<char>& mergedNulls,
      folly::Executor* executor)
      : FlatMapFieldReaderBase<T, hasNull>(
            pool,
            std::move(type),
            decoder,
            std::move(keyNodes),
            boolBuffer),
        mergedNulls_{mergedNulls},
        executor_{executor} {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    uint64_t rowCount{0};
    if constexpr (hasNull) {
      NIMBLE_ASSERT(
          FieldReader::decoder_ != nullptr,
          "decoder_ should be set when hasNull is true");
      const auto* encoding = FieldReader::decoder_->encoding();
      NIMBLE_CHECK(
          encoding != nullptr,
          "Decoder must be loaded for output size estimation.");
      rowCount = encoding->rowCount();
      // Adding memory for velox::BaseVector::nulls_
      totalBytes += rowCount / 8;
    }

    for (const auto& node : this->keyNodes_) {
      if (node == nullptr) {
        // This could happen when selected feature does not exist.
        continue;
      }
      auto keyNodeSize = node->valueReader()->estimatedRowSize();
      if (!keyNodeSize.has_value()) {
        return std::nullopt;
      }
      const auto nonNullCount = keyNodeSize.value().first;
      const auto keyNodeBytesPerRow = keyNodeSize.value().second;
      totalBytes += keyNodeBytesPerRow * nonNullCount;
      // Adding memory for additional null overhead in outer layer
      if constexpr (hasNull) {
        NIMBLE_ASSERT(
            rowCount >= nonNullCount,
            fmt::format(
                "rowCount {} should be >= nonNullCount {}",
                rowCount,
                nonNullCount));
        totalBytes += (rowCount - nonNullCount) *
            nullOverheadBits(node->valueReader()->type()) / 8;
      } else if (rowCount == 0) {
        rowCount = nonNullCount;
      } else {
        NIMBLE_CHECK(
            rowCount == nonNullCount,
            fmt::format(
                "rowCount {} should be equal to nonNullCount {} under no null "
                "condition",
                rowCount,
                nonNullCount));
      }
    }
    return rowCount == 0 ? std::optional<std::pair<uint32_t, uint64_t>>({0, 0})
                         : std::optional<std::pair<uint32_t, uint64_t>>(
                               {rowCount, totalBytes / rowCount});
  }

  void next(
      uint32_t rowCount,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    NIMBLE_ASSERT(scatterBitmap == nullptr, "unexpected scatterBitmap");
    auto vector = VectorInitializer<velox::RowVector>::initialize(
        &this->pool_, output, this->type_, rowCount);
    vector->unsafeResize(rowCount);
    uint32_t nonNullCount = this->loadNulls(rowCount, vector);

    if (executor_) {
      for (uint32_t i = 0; i < this->keyNodes_.size(); ++i) {
        if (this->keyNodes_[i] == nullptr) {
          this->ensureNullConstant(
              rowCount, vector->childAt(i), this->type_->childAt(i));
        } else {
          executor_->add([this,
                          rowCount,
                          nonNullCount,
                          &node = this->keyNodes_[i],
                          &child = vector->childAt(i)] {
            node->readAsChild(child, rowCount, nonNullCount, this->boolBuffer_);
          });
        }
      }
    } else {
      for (uint32_t i = 0; i < this->keyNodes_.size(); ++i) {
        if (this->keyNodes_[i] == nullptr) {
          this->ensureNullConstant(
              rowCount, vector->childAt(i), this->type_->childAt(i));
        } else {
          this->keyNodes_[i]->readAsChild(
              vector->childAt(i),
              rowCount,
              nonNullCount,
              this->boolBuffer_,
              &mergedNulls_);
        }
      }
    }
  }

 private:
  Vector<char>& mergedNulls_;
  folly::Executor* executor_;
};

template <typename T>
class StructFlatMapFieldReaderFactory final
    : public FlatMapFieldReaderFactoryBase<T> {
  template <bool hasNull>
  using ReaderType = StructFlatMapFieldReader<T, hasNull>;

 public:
  StructFlatMapFieldReaderFactory(
      velox::memory::MemoryPool& pool,
      velox::TypePtr veloxType,
      const Type* type,
      std::vector<const StreamDescriptor*> inMapDescriptors,
      std::vector<std::unique_ptr<FieldReaderFactory>> valueReaders,
      const std::vector<size_t>& selectedChildren,
      folly::Executor* executor)
      : FlatMapFieldReaderFactoryBase<T>(
            pool,
            std::move(veloxType),
            type,
            std::move(inMapDescriptors),
            std::move(valueReaders),
            selectedChildren),
        mergedNulls_{&this->pool_},
        executor_{executor} {
    NIMBLE_ASSERT(this->nimbleType_->isFlatMap(), "Type should be a flat map.");
  }

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return this->template createFlatMapReader<ReaderType, true>(
        decoders, mergedNulls_, executor_);
  }

 private:
  Vector<char> mergedNulls_;
  folly::Executor* executor_;
};

template <typename T, bool hasNull>
class MergedFlatMapFieldReader final
    : public FlatMapFieldReaderBase<T, hasNull> {
 public:
  MergedFlatMapFieldReader(
      velox::memory::MemoryPool& pool,
      velox::TypePtr type,
      Decoder* decoder,
      std::vector<std::unique_ptr<FlatMapKeyNode<T>>> keyNodes,
      Vector<bool>& boolBuffer)
      : FlatMapFieldReaderBase<T, hasNull>(
            pool,
            std::move(type),
            decoder,
            std::move(keyNodes),
            boolBuffer) {}

  std::optional<std::pair<uint32_t, uint64_t>> estimatedRowSize() const final {
    uint64_t totalBytes{0};
    uint32_t rowCount{0};
    if constexpr (hasNull) {
      NIMBLE_ASSERT(
          FieldReader::decoder_ != nullptr,
          "decoder_ should be set when hasNull is true");
      const auto* encoding = FieldReader::decoder_->encoding();
      NIMBLE_CHECK(
          encoding != nullptr,
          "Decoder must be loaded for output size estimation.");
      rowCount = encoding->rowCount();
      // Adding memory for velox::BaseVector::nulls_
      totalBytes += rowCount / 8;
    } else {
      if (this->keyNodes_.empty()) {
        // This happens when selected feature does not exist in the flatmap. As
        // we cannot acquire row count in this case, nullopt will be returned to
        // indicate unsupported.
        return std::nullopt;
      }
      const auto& keyNode = this->keyNodes_.back();
      VELOX_CHECK_NOT_NULL(keyNode, "keyNode should not be null");
      rowCount = keyNode->inMapDecoder()->encoding()->rowCount();
    }

    // Adding memory for velox::ArrayVectorBase::offsets_ and
    // velox::ArrayVectorBase::sizes_
    totalBytes += rowCount * sizeof(int32_t) * 2;

    // Estimation of map key vector size in velox::MapVector.
    // Adding memory for key vector's BaseVector::nulls_
    totalBytes += rowCount * this->keyNodes_.size() / 8;
    // MergedFlatMap key field is either velox::StringView or primative type
    uint64_t totalKeyBytesPerRow{0};
    if constexpr (std::is_same<T, velox::StringView>::value) {
      for (const auto& node : this->keyNodes_) {
        const auto keyBytes = node->key().get().size();
        // Adding memory for key vector's velox::FlatVector::stringBuffers_
        totalKeyBytesPerRow += keyBytes;
        // Adding overheads for StringView in velox::FlatVector::values_
        totalKeyBytesPerRow +=
            keyBytes > velox::StringView::kInlineSize ? 16 : 4;
      }
    } else {
      // Adding memory for key vector's velox::FlatVector::values_
      totalKeyBytesPerRow += this->keyNodes_.size() * sizeof(T);
    }
    // Null row count in this map cannot be easily obtained, we over-estimate by
    // multiplying total row count.
    totalBytes += rowCount * totalKeyBytesPerRow;

    // Estimation of map value vector size in velox::MapVector. As
    // MergedFlatMapReader transforms the dimension of the keys and values from
    // a flat map representation to velox map representation, there is no easy
    // way of doing a direct estimation of velox values column. So we adopt the
    // ways from StructFlatMapReader for estimating values size.
    for (const auto& node : this->keyNodes_) {
      auto valueSize = node->valueReader()->estimatedRowSize();
      if (!valueSize.has_value()) {
        return std::nullopt;
      }
      const auto nonNullCount = valueSize.value().first;
      const auto valueBytesPerRow = valueSize.value().second;
      totalBytes += nonNullCount * valueBytesPerRow;
      if constexpr (hasNull) {
        NIMBLE_ASSERT(
            rowCount >= nonNullCount,
            fmt::format(
                "rowCount {} should be >= nonNullCount {}",
                rowCount,
                nonNullCount));
        // Adding null overhead on outer layer
        totalBytes += (rowCount - nonNullCount) *
            nullOverheadBits(node->valueReader()->type()) / 8;
      }
    }
    return rowCount == 0 ? std::optional<std::pair<uint32_t, uint64_t>>({0, 0})
                         : std::optional<std::pair<uint32_t, uint64_t>>(
                               {rowCount, totalBytes / rowCount});
  }

  void next(
      uint32_t rowCount,
      velox::VectorPtr& output,
      const bits::Bitmap* scatterBitmap) final {
    NIMBLE_ASSERT(scatterBitmap == nullptr, "unexpected scatterBitmap");
    auto vector = VectorInitializer<velox::MapVector>::initialize(
        &this->pool_, output, this->type_, rowCount);
    vector->resize(rowCount);
    velox::VectorPtr& keysVector = vector->mapKeys();
    // Check the refCount for key vector
    auto flatKeysVector = VectorInitializer<velox::FlatVector<T>>::initialize(
        &this->pool_,
        keysVector,
        std::static_pointer_cast<const velox::MapType>(this->type_)->keyType(),
        rowCount);

    velox::BufferPtr offsets = vector->mutableOffsets(rowCount);
    velox::BufferPtr lengths = vector->mutableSizes(rowCount);
    uint32_t nonNullCount = this->loadNulls(rowCount, vector);

    nodes_.clear();
    size_t totalChildren = 0;
    for (auto& node : this->keyNodes_) {
      auto numValues = node->readInMapData(nonNullCount);
      if (numValues > 0) {
        nodes_.push_back(node.get());
        totalChildren += numValues;
      }
    }

    velox::VectorPtr nodeValues;
    velox::VectorPtr& valuesVector = vector->mapValues();
    if (totalChildren > 0) {
      keysVector->resize(totalChildren, false);
      velox::BaseVector::prepareForReuse(valuesVector, totalChildren);
    }

    auto* offsetsPtr = offsets->asMutable<velox::vector_size_t>();
    auto* lengthsPtr = lengths->asMutable<velox::vector_size_t>();
    initRowWiseInMap(rowCount);
    initOffsets(rowCount, offsetsPtr, lengthsPtr);

    // Always access inMap and value streams node-wise to avoid large striding
    // through the memory and destroying CPU cache performance.
    //
    // Index symbology used in this class:
    // i : Row index
    // j : Node index
    for (size_t j = 0; j < nodes_.size(); ++j) {
      copyRanges_.clear();
      for (velox::vector_size_t i = 0; i < rowCount; ++i) {
        if (!velox::bits::isBitSet(
                rowWiseInMap_.data(), j + i * nodes_.size())) {
          continue;
        }
        const velox::vector_size_t sourceIndex = copyRanges_.size();
        copyRanges_.push_back({sourceIndex, offsetsPtr[i], 1});
        flatKeysVector->set(offsetsPtr[i], nodes_[j]->key().get());
        ++offsetsPtr[i];
      }
      nodes_[j]->loadValues(nodeValues);
      valuesVector->copyRanges(nodeValues.get(), copyRanges_);
    }
    if (rowCount > 0) {
      NIMBLE_ASSERT(
          offsetsPtr[rowCount - 1] == totalChildren,
          "Total map entry size mismatch");
      // We updated `offsetsPtr' during the copy process, so that now it was
      // shifted to the left by 1 element (i.e. offsetsPtr[i] is really
      // offsetsPtr[i+1]).  Need to restore the values back to their correct
      // positions.
      std::copy_backward(
          offsetsPtr, offsetsPtr + rowCount - 1, offsetsPtr + rowCount);
      offsetsPtr[0] = 0;
    }

    // Reset the updated value vector to result
    vector->setKeysAndValues(std::move(keysVector), std::move(valuesVector));
  }

 private:
  void initRowWiseInMap(velox::vector_size_t rowCount) {
    rowWiseInMap_.resize(velox::bits::nwords(nodes_.size() * rowCount));
    std::fill(rowWiseInMap_.begin(), rowWiseInMap_.end(), 0);
    for (size_t j = 0; j < nodes_.size(); ++j) {
      uint32_t inMapIndex = 0;
      for (velox::vector_size_t i = 0; i < rowCount; ++i) {
        const bool isNull = hasNull && !this->boolBuffer_[i];
        if (!isNull && nodes_[j]->inMap(inMapIndex)) {
          velox::bits::setBit(rowWiseInMap_.data(), j + i * nodes_.size());
        }
        inMapIndex += !isNull;
      }
    }
  }

  void initOffsets(
      velox::vector_size_t rowCount,
      velox::vector_size_t* offsets,
      velox::vector_size_t* lengths) {
    velox::vector_size_t offset = 0;
    for (velox::vector_size_t i = 0; i < rowCount; ++i) {
      offsets[i] = offset;
      lengths[i] = velox::bits::countBits(
          rowWiseInMap_.data(), i * nodes_.size(), (i + 1) * nodes_.size());
      offset += lengths[i];
    }
  }

  // All the nodes that is selected to be read.
  std::vector<FlatMapKeyNode<T>*> nodes_;

  // In-map mask (1 bit per value), organized in row first layout.
  std::vector<uint64_t> rowWiseInMap_;

  // Copy ranges from one node values into the merged values.
  std::vector<velox::BaseVector::CopyRange> copyRanges_;
};

template <typename T>
class MergedFlatMapFieldReaderFactory final
    : public FlatMapFieldReaderFactoryBase<T> {
  template <bool hasNull>
  using ReaderType = MergedFlatMapFieldReader<T, hasNull>;

 public:
  using FlatMapFieldReaderFactoryBase<T>::FlatMapFieldReaderFactoryBase;

  std::unique_ptr<FieldReader> createReader(
      const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders)
      final {
    return this->template createFlatMapReader<ReaderType, false>(decoders);
  }
};

std::unique_ptr<FieldReaderFactory> createFlatMapReaderFactory(
    velox::memory::MemoryPool& pool,
    velox::TypeKind keyKind,
    velox::TypePtr veloxType,
    const Type* type,
    std::vector<const StreamDescriptor*> inMapDescriptors,
    std::vector<std::unique_ptr<FieldReaderFactory>> valueReaders,
    const std::vector<size_t>& selectedChildren,
    bool flatMapAsStruct,
    folly::Executor* executor) {
  switch (keyKind) {
#define SCALAR_CASE(veloxKind, fieldType)                                  \
  case velox::TypeKind::veloxKind: {                                       \
    if (flatMapAsStruct) {                                                 \
      return std::make_unique<StructFlatMapFieldReaderFactory<fieldType>>( \
          pool,                                                            \
          std::move(veloxType),                                            \
          type,                                                            \
          std::move(inMapDescriptors),                                     \
          std::move(valueReaders),                                         \
          selectedChildren,                                                \
          executor);                                                       \
    } else {                                                               \
      return std::make_unique<MergedFlatMapFieldReaderFactory<fieldType>>( \
          pool,                                                            \
          std::move(veloxType),                                            \
          type,                                                            \
          std::move(inMapDescriptors),                                     \
          std::move(valueReaders),                                         \
          selectedChildren);                                               \
    }                                                                      \
  }

    SCALAR_CASE(TINYINT, int8_t);
    SCALAR_CASE(SMALLINT, int16_t);
    SCALAR_CASE(INTEGER, int32_t);
    SCALAR_CASE(BIGINT, int64_t);
    SCALAR_CASE(VARCHAR, velox::StringView);
    SCALAR_CASE(VARBINARY, velox::StringView);
#undef SCALAR_CASE

    default:
      NIMBLE_NOT_SUPPORTED(
          fmt::format("Not supported flatmap key type: {} ", keyKind));
  }
}

std::shared_ptr<const velox::Type> createFlatType(
    const std::vector<std::string>& selectedFeatures,
    const velox::TypePtr& veloxType) {
  NIMBLE_ASSERT(
      !selectedFeatures.empty(),
      "Empty feature selection not allowed for struct encoding.");

  auto& valueType = veloxType->asMap().valueType();
  return velox::ROW(
      std::vector<std::string>(selectedFeatures),
      std::vector<std::shared_ptr<const velox::Type>>(
          selectedFeatures.size(), valueType));
}

velox::TypePtr inferType(
    const FieldReaderParams& params,
    const std::string& name,
    const velox::TypePtr& type,
    size_t level) {
  // Special case for flatmaps. If the flatmap field is missing, still need to
  // honor the "as struct" intent by returning row instead of map.
  if (level == 1 && params.readFlatMapFieldAsStruct.count(name) > 0) {
    NIMBLE_CHECK(
        type->kind() == velox::TypeKind::MAP,
        "Unexpected type kind of flat maps.");
    auto it = params.flatMapFeatureSelector.find(name);
    NIMBLE_CHECK(
        it != params.flatMapFeatureSelector.end() &&
            !it->second.features.empty(),
        fmt::format(
            "Flat map feature selection for map '{}' has empty feature set.",
            name));
    NIMBLE_CHECK(
        it->second.mode == SelectionMode::Include,
        "Flat map exclusion list is not supported when flat map field is missing.");

    return createFlatType(it->second.features, type);
  }
  return type;
}

std::unique_ptr<FieldReaderFactory> createFieldReaderFactory(
    const FieldReaderParams& parameters,
    velox::memory::MemoryPool& pool,
    const std::shared_ptr<const Type>& nimbleType,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& veloxType,
    std::vector<uint32_t>& offsets,
    const std::function<bool(uint32_t)>& isSelected,
    folly::Executor* executor,
    size_t level = 0,
    const std::string* name = nullptr) {
  auto veloxKind = veloxType->type()->kind();
  // compatibleKinds are the types that can be upcasted to nimbleType
  auto checkType = [&nimbleType](
                       const std::vector<ScalarKind>& compatibleKinds) {
    return std::any_of(
        compatibleKinds.begin(),
        compatibleKinds.end(),
        [&nimbleType](ScalarKind k) {
          return nimbleType->asScalar().scalarDescriptor().scalarKind() == k;
        });
  };

// Assuming no-upcasting is the most common case, putting the largest type size
// at the beginning so the compatibility check can finish quicker.
#define BOOLEAN_COMPATIBLE \
  { ScalarKind::Bool }
#define TINYINT_COMPATIBLE \
  { ScalarKind::Int8, ScalarKind::Bool }
#define SMALLINT_COMPATIBLE \
  { ScalarKind::Int16, ScalarKind::Int8, ScalarKind::Bool }
#define INTEGER_COMPATIBLE \
  { ScalarKind::Int32, ScalarKind::Int16, ScalarKind::Int8, ScalarKind::Bool }
#define BIGINT_COMPATIBLE                                                      \
  {                                                                            \
    ScalarKind::Int64, ScalarKind::Int32, ScalarKind::Int16, ScalarKind::Int8, \
        ScalarKind::Bool                                                       \
  }
#define FLOAT_COMPATIBLE \
  { ScalarKind::Float }
#define DOUBLE_COMPATIBLE \
  { ScalarKind::Double, ScalarKind::Float }

  switch (veloxKind) {
#define SCALAR_CASE(veloxKind, cppType, compitableKinds)                   \
  case velox::TypeKind::veloxKind: {                                       \
    NIMBLE_CHECK(                                                          \
        nimbleType->isScalar() && checkType(compitableKinds),              \
        "Provided schema doesn't match file schema.");                     \
    offsets.push_back(nimbleType->asScalar().scalarDescriptor().offset()); \
    return std::make_unique<ScalarFieldReaderFactory<cppType>>(            \
        pool, veloxType->type(), nimbleType.get());                        \
  }

    SCALAR_CASE(BOOLEAN, bool, BOOLEAN_COMPATIBLE);
    SCALAR_CASE(TINYINT, int8_t, TINYINT_COMPATIBLE);
    SCALAR_CASE(SMALLINT, int16_t, SMALLINT_COMPATIBLE);
    SCALAR_CASE(INTEGER, int32_t, INTEGER_COMPATIBLE);
    SCALAR_CASE(BIGINT, int64_t, BIGINT_COMPATIBLE);
    SCALAR_CASE(REAL, float, FLOAT_COMPATIBLE);
    SCALAR_CASE(DOUBLE, double, DOUBLE_COMPATIBLE);
#undef SCALAR_CASE

    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::VARBINARY: {
      NIMBLE_CHECK(
          nimbleType->isScalar() &&
              (veloxKind == velox::TypeKind::VARCHAR &&
                   nimbleType->asScalar().scalarDescriptor().scalarKind() ==
                       ScalarKind::String ||
               veloxKind == velox::TypeKind::VARBINARY &&
                   nimbleType->asScalar().scalarDescriptor().scalarKind() ==
                       ScalarKind::Binary),
          "Provided schema doesn't match file schema.");
      offsets.push_back(nimbleType->asScalar().scalarDescriptor().offset());
      return std::make_unique<StringFieldReaderFactory>(
          pool, veloxType->type(), nimbleType.get());
    }
    case velox::TypeKind::ARRAY: {
      NIMBLE_CHECK(
          nimbleType->isArray() || nimbleType->isArrayWithOffsets(),
          "Provided schema doesn't match file schema.");
      NIMBLE_ASSERT(
          veloxType->size() == 1,
          "Velox array type should have exactly one child.");
      if (nimbleType->isArray()) {
        auto& nimbleArray = nimbleType->asArray();
        auto& elementType = veloxType->childAt(0);
        offsets.push_back(nimbleArray.lengthsDescriptor().offset());
        auto elements = isSelected(elementType->id())
            ? createFieldReaderFactory(
                  parameters,
                  pool,
                  nimbleArray.elements(),
                  elementType,
                  offsets,
                  isSelected,
                  executor,
                  level + 1)
            : std::make_unique<NullFieldReaderFactory>(
                  pool, elementType->type());
        return std::make_unique<ArrayFieldReaderFactory>(
            pool, veloxType->type(), nimbleType.get(), std::move(elements));
      } else {
        auto& nimbleArrayWithOffsets = nimbleType->asArrayWithOffsets();
        offsets.push_back(nimbleArrayWithOffsets.lengthsDescriptor().offset());
        offsets.push_back(nimbleArrayWithOffsets.offsetsDescriptor().offset());

        auto& elementType = veloxType->childAt(0);
        auto elements = isSelected(elementType->id())
            ? createFieldReaderFactory(
                  parameters,
                  pool,
                  nimbleArrayWithOffsets.elements(),
                  elementType,
                  offsets,
                  isSelected,
                  executor,
                  level + 1)
            : std::make_unique<NullFieldReaderFactory>(
                  pool, elementType->type());
        return std::make_unique<ArrayWithOffsetsFieldReaderFactory>(
            pool, veloxType->type(), nimbleType.get(), std::move(elements));
      }
    }
    case velox::TypeKind::ROW: {
      NIMBLE_CHECK(
          nimbleType->isRow(), "Provided schema doesn't match file schema.");

      auto& nimbleRow = nimbleType->asRow();
      auto& veloxRow = veloxType->type()->as<velox::TypeKind::ROW>();
      std::vector<std::unique_ptr<FieldReaderFactory>> children;
      std::vector<velox::TypePtr> childTypes;
      children.reserve(veloxType->size());
      childTypes.reserve(veloxType->size());
      offsets.push_back(nimbleRow.nullsDescriptor().offset());

      for (auto i = 0; i < veloxType->size(); ++i) {
        auto& child = veloxType->childAt(i);
        std::unique_ptr<FieldReaderFactory> factory;
        if (isSelected(child->id())) {
          if (i < nimbleRow.childrenCount()) {
            factory = createFieldReaderFactory(
                parameters,
                pool,
                nimbleRow.childAt(i),
                child,
                offsets,
                isSelected,
                executor,
                level + 1,
                &veloxRow.nameOf(i));
          } else {
            factory = std::make_unique<NullFieldReaderFactory>(
                pool,
                inferType(
                    parameters,
                    veloxRow.nameOf(i),
                    veloxRow.childAt(i),
                    level + 1));
          }
        }
        childTypes.push_back(factory ? factory->veloxType() : child->type());
        children.push_back(std::move(factory));
      }

      // Underlying reader may return a different vector type than what's
      // specified (eg. flat map read as struct). So create new ROW type based
      // on child types. Note this special logic is only for Row type based
      // on the constraint that flatmap can only be top level fields.
      return std::make_unique<RowFieldReaderFactory>(
          pool,
          velox::ROW(
              std::vector<std::string>(veloxRow.names()),
              std::move(childTypes)),
          nimbleType.get(),
          std::move(children),
          executor);
    }
    case velox::TypeKind::MAP: {
      NIMBLE_CHECK(
          nimbleType->isMap() || nimbleType->isFlatMap() ||
              nimbleType->isSlidingWindowMap(),
          "Provided schema doesn't match file schema.");
      NIMBLE_ASSERT(
          veloxType->size() == 2,
          "Velox map type should have exactly two children.");

      if (nimbleType->isMap()) {
        const auto& nimbleMap = nimbleType->asMap();
        auto& keyType = veloxType->childAt(0);
        offsets.push_back(nimbleMap.lengthsDescriptor().offset());
        auto keys = isSelected(keyType->id())
            ? createFieldReaderFactory(
                  parameters,
                  pool,
                  nimbleMap.keys(),
                  keyType,
                  offsets,
                  isSelected,
                  executor,
                  level + 1)
            : std::make_unique<NullFieldReaderFactory>(pool, keyType->type());
        auto& valueType = veloxType->childAt(1);
        auto values = isSelected(valueType->id())
            ? createFieldReaderFactory(
                  parameters,
                  pool,
                  nimbleMap.values(),
                  valueType,
                  offsets,
                  isSelected,
                  executor,
                  level + 1)
            : std::make_unique<NullFieldReaderFactory>(pool, valueType->type());
        return std::make_unique<MapFieldReaderFactory>(
            pool,
            veloxType->type(),
            nimbleType.get(),
            std::move(keys),
            std::move(values));
      } else if (nimbleType->isSlidingWindowMap()) {
        const auto& nimbleMap = nimbleType->asSlidingWindowMap();
        offsets.push_back(nimbleMap.offsetsDescriptor().offset());
        offsets.push_back(nimbleMap.lengthsDescriptor().offset());
        auto& keyType = veloxType->childAt(0);
        auto keys = isSelected(keyType->id())
            ? createFieldReaderFactory(
                  parameters,
                  pool,
                  nimbleMap.keys(),
                  keyType,
                  offsets,
                  isSelected,
                  executor,
                  level + 1)
            : std::make_unique<NullFieldReaderFactory>(pool, keyType->type());
        auto& valueType = veloxType->childAt(1);
        auto values = isSelected(valueType->id())
            ? createFieldReaderFactory(
                  parameters,
                  pool,
                  nimbleMap.values(),
                  valueType,
                  offsets,
                  isSelected,
                  executor,
                  level + 1)
            : std::make_unique<NullFieldReaderFactory>(pool, valueType->type());
        return std::make_unique<SlidingWindowMapFieldReaderFactory>(
            pool,
            veloxType->type(),
            nimbleType.get(),
            std::move(keys),
            std::move(values));
      } else {
        auto& nimbleFlatMap = nimbleType->asFlatMap();
        offsets.push_back(nimbleFlatMap.nullsDescriptor().offset());
        NIMBLE_CHECK(
            level == 1 && name != nullptr,
            "Flat map is only supported as top level fields");
        auto flatMapAsStruct =
            parameters.readFlatMapFieldAsStruct.count(*name) > 0;

        // Extract features only when flat map is not empty. When flatmap is
        // empty, writer creates dummy child with empty name to carry schema
        // information. We need to capture actual children count here.
        auto childrenCount = nimbleFlatMap.childrenCount();
        if (childrenCount == 1 && nimbleFlatMap.nameAt(0).empty()) {
          childrenCount = 0;
        }

        folly::F14FastMap<std::string_view, size_t> namesToIndices;

        auto featuresIt = parameters.flatMapFeatureSelector.find(*name);
        auto hasFeatureSelection =
            featuresIt != parameters.flatMapFeatureSelector.end();
        if (hasFeatureSelection) {
          NIMBLE_CHECK(
              !featuresIt->second.features.empty(),
              fmt::format(
                  "Flat map feature selection for map '{}' has empty feature set.",
                  *name));

          if (featuresIt->second.mode == SelectionMode::Include) {
            // We have valid feature projection. Build name -> index lookup
            // table.
            namesToIndices.reserve(childrenCount);
            for (auto i = 0; i < childrenCount; ++i) {
              namesToIndices.emplace(nimbleFlatMap.nameAt(i), i);
            }
          } else {
            NIMBLE_CHECK(
                !flatMapAsStruct,
                fmt::format(
                    "Exclusion can only be applied when flat map is returned as a regular map."));
          }
        } else {
          // Not specifying features for a flat map is only allowed when
          // reconstructing a map column. For struct encoding, we require the
          // caller to provide feature selection, as it dictates the order of
          // the returned features.
          NIMBLE_CHECK(
              !flatMapAsStruct,
              fmt::format(
                  "Flat map '{}' is configured to be returned as a struct, but feature selection is missing. "
                  "Feature selection is used to define the order of the features in the returned struct.",
                  *name));
        }

        auto actualType = veloxType->type();
        auto& valueType = veloxType->childAt(1);
        std::vector<size_t> selectedChildren;
        std::vector<const StreamDescriptor*> inMapDescriptors;

        if (flatMapAsStruct) {
          // When reading as struct, all children appear in the feature
          // selection will need to be in the result even if they don't exist in
          // the schema.
          auto& features = featuresIt->second.features;
          selectedChildren.reserve(features.size());
          inMapDescriptors.reserve(features.size());
          actualType = createFlatType(features, veloxType->type());

          for (const auto& feature : features) {
            auto it = namesToIndices.find(feature);
            if (it != namesToIndices.end()) {
              auto childIdx = it->second;
              selectedChildren.push_back(childIdx);
              auto* inMapDescriptor =
                  &nimbleFlatMap.inMapDescriptorAt(childIdx);
              inMapDescriptors.push_back(inMapDescriptor);
              offsets.push_back(inMapDescriptor->offset());
            } else {
              inMapDescriptors.push_back(nullptr);
            }
          }
        } else if (childrenCount > 0) {
          // When reading as regular map, projection only matters if the map is
          // not empty.
          if (!hasFeatureSelection) {
            selectedChildren.reserve(childrenCount);
            for (auto i = 0; i < childrenCount; ++i) {
              selectedChildren.push_back(i);
            }
          } else {
            auto& features = featuresIt->second.features;
            if (featuresIt->second.mode == SelectionMode::Include) {
              // Note this path is slightly different from "as struct" path as
              // it doesn't need to add the missing children to the selection.
              selectedChildren.reserve(features.size());
              for (auto& feature : features) {
                auto it = namesToIndices.find(feature);
                if (it != namesToIndices.end()) {
                  selectedChildren.push_back(it->second);
                }
              }
            } else {
              folly::F14FastSet<std::string_view> exclusions(
                  features.begin(), features.end());
              selectedChildren.reserve(childrenCount);
              for (auto i = 0; i < childrenCount; ++i) {
                if (exclusions.count(nimbleFlatMap.nameAt(i)) == 0) {
                  selectedChildren.push_back(i);
                }
              }
            }
          }

          inMapDescriptors.reserve(selectedChildren.size());
          for (auto childIdx : selectedChildren) {
            auto* inMapDescriptor = &nimbleFlatMap.inMapDescriptorAt(childIdx);
            inMapDescriptors.push_back(inMapDescriptor);
            offsets.push_back(inMapDescriptor->offset());
          }
        }

        std::vector<std::unique_ptr<FieldReaderFactory>> valueReaders;
        valueReaders.reserve(selectedChildren.size());
        for (auto childIdx : selectedChildren) {
          valueReaders.push_back(createFieldReaderFactory(
              parameters,
              pool,
              nimbleFlatMap.childAt(childIdx),
              valueType,
              offsets,
              isSelected,
              executor,
              level + 1));
        }

        auto& keySelectionCallback = parameters.keySelectionCallback;
        if (keySelectionCallback) {
          keySelectionCallback(
              {.totalKeys = childrenCount,
               .selectedKeys = selectedChildren.size()});
        }

        return createFlatMapReaderFactory(
            pool,
            veloxType->childAt(0)->type()->kind(),
            std::move(actualType),
            nimbleType.get(),
            std::move(inMapDescriptors),
            std::move(valueReaders),
            selectedChildren,
            flatMapAsStruct,
            executor);
      }
    }
    default:
      NIMBLE_NOT_SUPPORTED(
          fmt::format("Unsupported type: {}", veloxType->type()->kindName()));
  }
}

} // namespace

void FieldReader::ensureNullConstant(
    uint32_t rowCount,
    velox::VectorPtr& output,
    const std::shared_ptr<const velox::Type>& type) const {
  // If output is already single referenced null constant, resize. Otherwise,
  // allocate new one.
  if (output && output.use_count() == 1 &&
      output->encoding() == velox::VectorEncoding::Simple::CONSTANT &&
      output->isNullAt(0)) {
    output->resize(rowCount);
  } else {
    output = velox::BaseVector::createNullConstant(type, rowCount, &pool_);
  }
}

void FieldReader::reset() {
  if (decoder_) {
    decoder_->reset();
  }
}

std::unique_ptr<FieldReader> FieldReaderFactory::createNullColumnReader()
    const {
  return std::make_unique<NullColumnReader>(pool_, veloxType_);
}

Decoder* FOLLY_NULLABLE FieldReaderFactory::getDecoder(
    const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders,
    const StreamDescriptor& streamDescriptor) const {
  auto it = decoders.find(streamDescriptor.offset());
  if (it == decoders.end()) {
    // It is possible that for a given offset, we don't have a matching
    // decoder. Each stripe might see different number of streams, so for all
    // unknown streams, there won't be a matching decoder.
    return nullptr;
  }

  return it->second.get();
}

template <typename T, typename... Args>
std::unique_ptr<FieldReader> FieldReaderFactory::createReaderImpl(
    const folly::F14FastMap<offset_size, std::unique_ptr<Decoder>>& decoders,
    const StreamDescriptor& nullsDescriptor,
    Args&&... args) const {
  auto decoder = getDecoder(decoders, nullsDescriptor);
  if (!decoder) {
    return createNullColumnReader();
  }

  return std::make_unique<T>(pool_, veloxType_, decoder, args()...);
}

std::unique_ptr<FieldReaderFactory> FieldReaderFactory::create(
    const FieldReaderParams& parameters,
    velox::memory::MemoryPool& pool,
    const std::shared_ptr<const Type>& nimbleType,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& veloxType,
    std::vector<uint32_t>& offsets,
    const std::function<bool(uint32_t)>& isSelected,
    folly::Executor* executor) {
  return createFieldReaderFactory(
      parameters, pool, nimbleType, veloxType, offsets, isSelected, executor);
}

} // namespace facebook::nimble
