// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/common/Bits.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "velox/common/memory/Memory.h"

#include <type_traits>

// The Encoding class defines an interface for interacting with encodings
// (aka vectors, aka arrays) of encoded data. The API is tailored for
// typical usage patterns within query engines, and is designed to be
// easily extensible.
//
// Some general notes:
//  1. Output bitmaps are compatible with the FixedBitArray class. In
//     particular this means you must use FixedBitArray::bufferSize to
//     determine the space needed for the bitmaps you pass to, e.g.,
//     the output buffer of Encoding::Equals.
//  2. When passing types into functions via the void* interfaces,
//     and when materializing data, numeric types are input/output via
//     their native types, while string are passed via std::string_view
//     -- NOT std::string!
//  3. We refer to the 'row pointer' in various places. Think of this as
//     the iterator over the encoding's data. Encoding::Reset() is like
//     rp = vec.begin(), advancing N rows is like rp = rp + N, etc.
//  4. An Encoding doesn't own the underlying data. If you want to create two
//     copies for some reason on the same data go ahead, they are totally
//     independent.
//  5. Each subclass of Encoding will provide its own serialization methods.
//     The resulting string should be passed to the subclass's constructor
//     at read time.
//  6. Although serialized Encodings can be stored independently if desired
//     (as they are just strings), when written to disk they are normally
//     stored together in a Tablet (see tablet.h).
//  7. We consistently use 4 byte unsigned integers for things like offsets
//     and row counts. Encodings, and indeed entire Tablets, are required to
//     fit within a uint32_t in size. This is enforced in the normal
//     ingestion pathways. If you directly use the serialization methods,
//     be careful that this remains true!
//  8. See the notes in the Encoding class about array encodings. These
//  encodings
//     have slightly different semantics than other encodings.

namespace facebook::nimble {

class Encoding {
 public:
  Encoding(velox::memory::MemoryPool& memoryPool, std::string_view data)
      : memoryPool_{memoryPool}, data_{data} {}
  virtual ~Encoding() = default;

  EncodingType encodingType() const;
  DataType dataType() const;
  uint32_t rowCount() const;

  static void copyIOBuf(char* pos, const folly::IOBuf& buf);

  virtual uint32_t nullCount() const {
    return 0;
  }

  // Resets the internal state (e.g. row pointer) to newly constructed form.
  virtual void reset() = 0;

  // Advances the row pointer N rows. Note that we don't provide a 'backwards'
  // iterator; if you need to move your row pointer back, reset() and skip().
  virtual void skip(uint32_t rowCount) = 0;

  // Materializes the next |rowCount| rows into buffer. Advances
  // the row pointer |rowCount|.
  //
  // Remember that buffer is interpreted as a physicalType*, with
  // std::string_view* for strings and bool* (not a bitmap) for bool. For
  // non-POD types, namely string data, note that the materialized values are
  // only guaranteed valid until the next non-const call to *this.
  //
  // If this->isNullable(), null rows are materialized as physicalType(). To be
  // able to distinguish between nulls and non-null physicalType() values, use
  // MaterializeNullable.
  virtual void materialize(uint32_t rowCount, void* buffer) = 0;

  // Nullable method.
  // Like Materialize, but also sets the ith bit of bitmap to reflect whether
  // ith row was null or not. 0 means null, 1 means not null. Null rows are left
  // untouched instead of being filled with default values.
  //
  // If |scatterBitmap| is provided, |rowCount| still indicates how many items
  // to be read from the encoding. However, instead of placing them sequentially
  // in the output |buffer|, the items are scattered. This means that item will
  // be place into the slot where the corresponding positional bit is set to 1
  // in |scatterBitmap|, (note that the value being read may be null). For every
  // positional scatter bit set to 0, it will fill a null in the poisition in
  // the output |buffer|. |rowCount| should match the number of bits set to 1 in
  // |scatterBitmap|. For scattered reads, |buffer| and |nulls| should
  // have enough space to accommodate |scatterBitmap.size()| items. When
  // |offset| is specified, use the |scatterBitmap| starting from |offset| and
  // scatter to |buffer| and |nulls| starting from |offset|.
  //
  // Returns number of items that are not null. In the case when all values are
  // non null, |nulls| will not be filled with all 1s. It's expected that
  // caller explicitly checks for that condition.
  virtual uint32_t materializeNullable(
      uint32_t rowCount,
      void* buffer,
      std::function<void*()> nulls,
      const bits::Bitmap* scatterBitmap = nullptr,
      uint32_t offset = 0) = 0;

  // Whether this encoding is nullable, i.e. contains any nulls. This property
  // modifies how engines need to interpret many of the function results, and a
  // number of functions are only callable if isNullable() returns true.
  virtual bool isNullable() const {
    return false;
  }

  // A number of functions are legal to call only if the encoding is dictionary
  // enabled, such as retrieving the dictionary indices or looking up a value
  // by dictionary index.
  virtual bool isDictionaryEnabled() const {
    return false;
  }

  // Dictionary method.
  // The size of the dictionary, which is equal to the number of unique values.
  virtual uint32_t dictionarySize() const {
    NIMBLE_NOT_SUPPORTED("Data is not dictionary encoded.");
  }

  // Dictionary method.
  // Returns the value at the given index, which must be in [0, num_entries).
  // This pointer is only guaranteed valid until the next Entry call.
  virtual const void* dictionaryEntry(uint32_t /* index */) const {
    NIMBLE_NOT_SUPPORTED("Data is not dictionary encoded.");
  }

  // Dictionary method.
  // Materializes the next |rowCount| dictionary indices into buffer. Advances
  // The row pointer |rowCount|.
  virtual void materializeIndices(
      uint32_t /* rowCount */,
      uint32_t* /* buffer */) {
    NIMBLE_NOT_SUPPORTED("Data is not dictionary encoded.");
  }

  // A string for debugging/iteration that gives details about *this.
  // Offset adds that many spaces before the msg (useful for children
  // encodings).
  virtual std::string debugString(int offset = 0) const;

 protected:
  // The binary layout for each Encoding begins with the same prefix:
  // 1 byte: EncodingType
  // 1 byte: DataType
  // 4 bytes: uint32_t num rows
  static constexpr int kEncodingTypeOffset = 0;
  static constexpr int kDataTypeOffset = 1;
  static constexpr int kRowCountOffset = 2;
  static constexpr int kPrefixSize = 6;

  static void serializePrefix(
      EncodingType encodingType,
      DataType dataType,
      uint32_t rowCount,
      char*& pos);

  velox::memory::MemoryPool& memoryPool_;
  const std::string_view data_;
};

// The TypedEncoding<physicalType> class exposes the same interface as the base
// Encoding class but provides common typed implementation for some apis exposed
// by the Encoding class. T means semantic data type, physicalType means data
// type used for encoding
template <typename T, typename physicalType>
class TypedEncoding : public Encoding {
  static_assert(
      std::is_same_v<physicalType, typename TypeTraits<T>::physicalType>);

 public:
  TypedEncoding(velox::memory::MemoryPool& memoryPool, std::string_view data)
      : Encoding{memoryPool, data} {}

  // Similar to materialize(), but scatters values to output buffer according to
  // scatterBitmap. When scatterBitmap is nullptr or all 1's, the output
  // nullBitmap will not be set. It's expected that caller explicitly checks
  // against the return value and handle such all 1's cases properly.
  uint32_t materializeNullable(
      uint32_t rowCount,
      void* buffer,
      std::function<void*()> nulls,
      const bits::Bitmap* scatterBitmap,
      uint32_t offset) override {
    // 1. Read X items from the encoding.
    // 2. Spread the items read in #1 into positions in |buffer| where
    // |scatterBitmap| has a matching positional bit set to 1.
    if (offset > 0) {
      buffer = static_cast<physicalType*>(buffer) + offset;
    }

    materialize(rowCount, buffer);

    // TODO: check rowCount matches the number of set bits in scatterBitmap.
    auto scatterCount =
        scatterBitmap ? scatterBitmap->size() - offset : rowCount;
    if (scatterCount == rowCount) {
      // No need to scatter. Avoid setting nullBitmap since caller is expected
      // to explicitly handle such non-null cases.
      return rowCount;
    }

    NIMBLE_CHECK(
        rowCount < scatterCount,
        fmt::format("Unexpected count {} vs {}", rowCount, scatterCount));

    void* nullBitmap = nulls();

    bits::BitmapBuilder nullBits{nullBitmap, offset + scatterCount};
    nullBits.copy(*scatterBitmap, offset, offset + scatterCount);

    // Scatter backward
    uint32_t pos = offset + scatterCount - 1;
    physicalType* output =
        static_cast<physicalType*>(buffer) + scatterCount - 1;
    const physicalType* source =
        static_cast<physicalType*>(buffer) + rowCount - 1;
    while (output != source) {
      if (scatterBitmap->test(pos)) {
        *output = *source;
        --source;
      }
      --output;
      --pos;
    }

    return rowCount;
  }
};

namespace detail {
template <typename T, uint16_t BufferSize>
class BufferedEncoding {
 public:
  explicit BufferedEncoding(std::unique_ptr<Encoding> encoding)
      : bufferPosition_{BufferSize},
        encodingPosition_{0},
        encoding_{std::move(encoding)} {}

  T nextValue() {
    if (UNLIKELY(bufferPosition_ == BufferSize)) {
      auto rows = std::min<uint32_t>(
          BufferSize, encoding_->rowCount() - encodingPosition_);
      encoding_->materialize(rows, buffer_.data());
      bufferPosition_ = 0;
    }
    ++encodingPosition_;
    return buffer_[bufferPosition_++];
  }

  void reset() {
    bufferPosition_ = BufferSize;
    encodingPosition_ = 0;
    encoding_->reset();
  }

  uint32_t position() const noexcept {
    return encodingPosition_ - 1;
  }

  uint32_t rowCount() const noexcept {
    return encoding_->rowCount();
  }

 private:
  uint16_t bufferPosition_;
  uint32_t encodingPosition_;
  std::unique_ptr<Encoding> encoding_;
  std::array<T, BufferSize> buffer_;
};

} // namespace detail
} // namespace facebook::nimble
