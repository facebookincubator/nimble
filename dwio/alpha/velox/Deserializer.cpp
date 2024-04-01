// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/Deserializer.h"
#include <zstd.h>
#include <zstd_errors.h>
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/velox/Decoder.h"
#include "dwio/alpha/velox/SchemaUtils.h"
#include "velox/dwio/common/TypeWithId.h"

namespace facebook::alpha {

namespace {

uint32_t getTypeStorageWidth(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar: {
      auto scalarKind = type.asScalar().scalarDescriptor().scalarKind();
      switch (scalarKind) {
        case ScalarKind::Bool:
        case ScalarKind::Int8:
        case ScalarKind::UInt8:
          return 1;
        case ScalarKind::Int16:
        case ScalarKind::UInt16:
          return 2;
        case ScalarKind::Int32:
        case ScalarKind::Float:
        case ScalarKind::UInt32:
          return 4;
        case ScalarKind::Int64:
        case ScalarKind::UInt64:
        case ScalarKind::Double:
          return 8;
        case ScalarKind::String:
        case ScalarKind::Binary:
        case ScalarKind::Undefined:
          return 0;
      }
      break;
    }
    case Kind::Row:
    case Kind::FlatMap:
      return 1;
    case Kind::Array:
    case Kind::ArrayWithOffsets:
    case Kind::Map:
      return 4;
  }
}

class DeserializerImpl : public Decoder {
 public:
  explicit DeserializerImpl(const Type& type) : type_{type} {}

  uint32_t next(
      uint32_t count,
      void* output,
      std::function<void*()> /* nulls */,
      const bits::Bitmap* /* scatterBitmap */) override {
    if (count == 0) {
      return count;
    }

    if (data_.empty()) {
      // TODO: This is less ideal. Need a way to avoid sending back the values.
      ALPHA_CHECK(
          type_.isRow() || type_.isFlatMap(),
          fmt::format("missing input data for {}", toString(type_.kind())));
      auto bools = static_cast<bool*>(output);
      std::fill(bools, bools + count, true);
      return count;
    }

    auto width = getTypeStorageWidth(type_);
    if (width > 0) {
      auto expectedSize = count * width;
      auto pos = data_.begin();
      auto compression = static_cast<CompressionType>(encoding::readChar(pos));
      switch (compression) {
        case CompressionType::Uncompressed: {
          ALPHA_CHECK(expectedSize == data_.end() - pos, "unexpected size");
          std::copy(pos, data_.end(), static_cast<char*>(output));
          break;
        }
        case CompressionType::Zstd: {
          // TODO: share compression implementation
          auto ret =
              ZSTD_decompress(output, expectedSize, pos, data_.size() - 1);
          ALPHA_CHECK(
              !ZSTD_isError(ret),
              fmt::format(
                  "Error uncompressing data: {}", ZSTD_getErrorName(ret)));
          ALPHA_CHECK(ret == expectedSize, "unexpected size");
          break;
        }
        default:
          ALPHA_NOT_SUPPORTED(
              fmt::format("Unsupported compression {}", toString(compression)));
      }
      return count;
    }

    // TODO: handle string compression. One option is to share implementations
    // with trivial encoding. That way, we also get bit packed booleans for
    // free.
    auto scalarKind = type_.asScalar().scalarDescriptor().scalarKind();
    ALPHA_CHECK(
        scalarKind == ScalarKind::String || scalarKind == ScalarKind::Binary,
        fmt::format("Unexpected scalar kind {}", toString(scalarKind)));
    auto sv = static_cast<std::string_view*>(output);
    auto pos = data_.data();
    for (auto i = 0; i < count; ++i) {
      sv[i] = encoding::readString(pos);
    }
    return count;
  }

  void skip(uint32_t /* count */) override {
    ALPHA_UNREACHABLE("unexpected call");
  }

  void reset() override {
    ALPHA_UNREACHABLE("unexpected call");
  }

  void reset(std::string_view data) {
    data_ = data;
  }

 private:
  const Type& type_;
  std::string_view data_;
};

const StreamDescriptor& getMainDescriptor(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar:
      return type.asScalar().scalarDescriptor();
    case Kind::Array:
      return type.asArray().lengthsDescriptor();
    case Kind::Map:
      return type.asMap().lengthsDescriptor();
    case Kind::Row:
      return type.asRow().nullsDescriptor();
    default:
      ALPHA_NOT_SUPPORTED(fmt::format(
          "Schema type {} is not supported.", toString(type.kind())));
  }
}

} // namespace

Deserializer::Deserializer(
    velox::memory::MemoryPool& pool,
    std::shared_ptr<const Type> schema)
    : pool_{pool}, schema_{std::move(schema)} {
  FieldReaderParams params;
  std::shared_ptr<const velox::dwio::common::TypeWithId> schemaWithId =
      velox::dwio::common::TypeWithId::create(convertToVeloxType(*schema_));
  std::vector<uint32_t> offsets;
  rootFactory_ =
      FieldReaderFactory::create(params, pool_, schema_, schemaWithId, offsets);
  SchemaReader::traverseSchema(schema_, [this](auto, auto& type, auto&) {
    deserializers_[getMainDescriptor(type).offset()] =
        std::make_unique<DeserializerImpl>(type);
  });
  rootReader_ = rootFactory_->createReader(deserializers_);
}

void Deserializer::deserialize(
    std::string_view data,
    velox::VectorPtr& vector) {
  auto pos = data.data();
  auto rows = encoding::readUint32(pos);
  for (uint32_t stream = 0; stream < deserializers_.size(); ++stream) {
    static_cast<DeserializerImpl*>(deserializers_[stream].get())
        ->reset(encoding::readString(pos));
  }
  ALPHA_CHECK(pos == data.end(), "unexpected end");
  rootReader_->next(rows, vector, nullptr);
}

} // namespace facebook::alpha
