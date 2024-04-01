// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
#include <algorithm>
#include <fstream>
#include <locale>
#include <numeric>
#include <ostream>
#include <tuple>
#include <utility>

#include "common/strings/Zstd.h"
#include "dwio/alpha/common/EncodingPrimitives.h"
#include "dwio/alpha/common/FixedBitArray.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"
#include "dwio/alpha/encodings/EncodingLayout.h"
#include "dwio/alpha/tools/AlphaDumpLib.h"
#include "dwio/alpha/tools/EncodingUtilities.h"
#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "folly/experimental/NestedCommandLineApp.h"

namespace facebook::alpha::tools {
#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define PURPLE "\033[35m"
#define CYAN "\033[36m"
#define RESET_COLOR "\033[0m"

namespace {

constexpr uint32_t kBufferSize = 1000;

struct GroupingKey {
  EncodingType encodingType;
  DataType dataType;
  std::optional<CompressionType> compressinType;
};

struct GroupingKeyCompare {
  size_t operator()(const GroupingKey& lhs, const GroupingKey& rhs) const {
    if (lhs.encodingType != rhs.encodingType) {
      return lhs.encodingType < rhs.encodingType;
    } else if (lhs.dataType != rhs.dataType) {
      return lhs.dataType < rhs.dataType;
    } else {
      return lhs.compressinType < rhs.compressinType;
    }
  }
};

class TableFormatter {
 public:
  TableFormatter(
      std::ostream& ostream,
      std::vector<std::tuple<
          std::string /* Title */,
          uint8_t /* Width */
          >> fields,
      bool noHeader = false)
      : ostream_{ostream}, fields_{std::move(fields)} {
    if (!noHeader) {
      ostream << YELLOW;
      for (const auto& field : fields_) {
        ostream << std::left << std::setw(std::get<1>(field) + 2)
                << std::get<0>(field);
      }
      ostream << RESET_COLOR << std::endl;
    }
  }

  void writeRow(const std::vector<std::string>& values) {
    assert(values.size() == fields_.size());
    for (auto i = 0; i < values.size(); ++i) {
      ostream_ << std::left << std::setw(std::get<1>(fields_[i]) + 2)
               << values[i];
    }
    ostream_ << std::endl;
  }

 private:
  std::ostream& ostream_;
  std::vector<std::tuple<
      std::string /* Title */,
      uint8_t /* Width */
      >>
      fields_;
};

void traverseTablet(
    velox::memory::MemoryPool& memoryPool,
    const Tablet& tablet,
    std::optional<int32_t> stripeIndex,
    std::function<void(uint32_t /* stripeId */)> stripeVisitor = nullptr,
    std::function<void(
        ChunkedStream& /*stream*/,
        uint32_t /*stripeId*/,
        uint32_t /* streamId*/)> streamVisitor = nullptr) {
  uint32_t startStripe = stripeIndex ? *stripeIndex : 0;
  uint32_t endStripe = stripeIndex ? *stripeIndex : tablet.stripeCount() - 1;
  for (uint32_t i = startStripe; i <= endStripe; ++i) {
    if (stripeVisitor) {
      stripeVisitor(i);
    }
    if (streamVisitor) {
      std::vector<uint32_t> identifiers(tablet.streamCount(i));
      std::iota(identifiers.begin(), identifiers.end(), 0);
      auto streams = tablet.load(i, {identifiers.cbegin(), identifiers.cend()});
      for (uint32_t j = 0; j < streams.size(); ++j) {
        auto& stream = streams[j];
        if (stream) {
          InMemoryChunkedStream chunkedStream{memoryPool, std::move(stream)};
          streamVisitor(chunkedStream, i, j);
        }
      }
    }
  }
}

template <typename T>
void printScalarData(
    std::ostream& ostream,
    velox::memory::MemoryPool& pool,
    Encoding& stream,
    uint32_t rowCount) {
  alpha::Vector<T> buffer(&pool);
  alpha::Vector<char> nulls(&pool);
  buffer.resize(rowCount);
  nulls.resize((alpha::FixedBitArray::bufferSize(rowCount, 1)));
  nulls.zero_out();
  if (stream.isNullable()) {
    stream.materializeNullable(
        rowCount, buffer.data(), [&]() { return nulls.data(); });
  } else {
    stream.materialize(rowCount, buffer.data());
    nulls.fill(0xff);
  }
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (stream.isNullable() && alpha::bits::getBit(i, nulls.data()) == 0) {
      ostream << "NULL" << std::endl;
    } else {
      ostream << folly::to<std::string>(buffer[i])
              << std::endl; // Have to use folly::to as Int8 was getting
                            // converted to char.
    }
  }
}

void printScalarType(
    std::ostream& ostream,
    velox::memory::MemoryPool& pool,
    Encoding& stream,
    uint32_t rowCount) {
  switch (stream.dataType()) {
#define CASE(KIND, cppType)                                    \
  case DataType::KIND: {                                       \
    printScalarData<cppType>(ostream, pool, stream, rowCount); \
    break;                                                     \
  }
    CASE(Int8, int8_t);
    CASE(Uint8, uint8_t);
    CASE(Int16, int16_t);
    CASE(Uint16, uint16_t);
    CASE(Int32, int32_t);
    CASE(Uint32, uint32_t);
    CASE(Int64, int64_t);
    CASE(Uint64, uint64_t);
    CASE(Float, float);
    CASE(Double, double);
    CASE(Bool, bool);
    CASE(String, std::string_view);
#undef CASE
    case DataType::Undefined: {
      ALPHA_UNREACHABLE(
          fmt::format("Undefined type for stream: {}", stream.dataType()));
    }
  }
}

template <typename T>
auto commaSeparated(T value) {
  return fmt::format(std::locale("en_US.UTF-8"), "{:L}", value);
}

} // namespace

AlphaDumpLib::AlphaDumpLib(std::ostream& ostream, const std::string& file)
    : pool_{velox::memory::deprecatedAddDefaultLeafMemoryPool()},
      file_{dwio::file_system::FileSystem::openForRead(
          file,
          dwio::common::request::AccessDescriptorBuilder()
              .withClientId("alpha_dump")
              .build())},
      ostream_{ostream} {}

void AlphaDumpLib::emitInfo() {
  auto tablet = std::make_shared<Tablet>(*pool_, file_.get());
  ostream_ << CYAN << "Alpha File " << RESET_COLOR << "Version "
           << tablet->majorVersion() << "." << tablet->minorVersion()
           << std::endl;
  ostream_ << "File Size: " << commaSeparated(tablet->fileSize()) << std::endl;
  ostream_ << "Checksum: " << tablet->checksum() << " ["
           << alpha::toString(tablet->checksumType()) << "]" << std::endl;
  ostream_ << "Footer Compression: " << tablet->footerCompressionType()
           << std::endl;
  ostream_ << "Footer Size: " << commaSeparated(tablet->footerSize())
           << std::endl;
  ostream_ << "Stripe Count: " << commaSeparated(tablet->stripeCount())
           << std::endl;
  ostream_ << "Row Count: " << commaSeparated(tablet->tabletRowCount())
           << std::endl;

  VeloxReader reader{*pool_, tablet};
  auto& metadata = reader.metadata();
  if (!metadata.empty()) {
    ostream_ << "Metadata:";
    for (const auto& pair : metadata) {
      ostream_ << std::endl << "  " << pair.first << ": " << pair.second;
    }
  }
  ostream_ << std::endl;
}

void AlphaDumpLib::emitSchema(bool collapseFlatMap) {
  auto tablet = std::make_shared<Tablet>(*pool_, file_.get());
  VeloxReader reader{*pool_, tablet};

  auto emitOffsets = [](const Type& type) {
    std::string offsets;
    switch (type.kind()) {
      case Kind::Scalar: {
        offsets =
            folly::to<std::string>(type.asScalar().scalarDescriptor().offset());
        break;
      }
      case Kind::Array: {
        offsets =
            folly::to<std::string>(type.asArray().lengthsDescriptor().offset());
        break;
      }
      case Kind::Map: {
        offsets =
            folly::to<std::string>(type.asMap().lengthsDescriptor().offset());
        break;
      }
      case Kind::Row: {
        offsets =
            folly::to<std::string>(type.asRow().nullsDescriptor().offset());
        break;
      }
      case Kind::FlatMap: {
        offsets =
            folly::to<std::string>(type.asFlatMap().nullsDescriptor().offset());
        break;
      }
      case Kind::ArrayWithOffsets: {
        offsets = "o:" +
            folly::to<std::string>(
                      type.asArrayWithOffsets().offsetsDescriptor().offset()) +
            ",l:" +
            folly::to<std::string>(
                      type.asArrayWithOffsets().lengthsDescriptor().offset());
        break;
      }
    }

    return offsets;
  };

  bool skipping = false;
  SchemaReader::traverseSchema(
      reader.schema(),
      [&](uint32_t level,
          const Type& type,
          const SchemaReader::NodeInfo& info) {
        auto parentType = info.parentType;
        if (parentType != nullptr && parentType->isFlatMap()) {
          auto childrenCount = parentType->asFlatMap().childrenCount();
          if (childrenCount > 2 && collapseFlatMap) {
            if (info.placeInSibling == 1) {
              ostream_ << std::string(
                              (std::basic_string<char>::size_type)level * 2,
                              ' ')
                       << "..." << std::endl;
              skipping = true;
            } else if (info.placeInSibling == childrenCount - 1) {
              skipping = false;
            }
          }
        }
        if (!skipping) {
          ostream_ << std::string(
                          (std::basic_string<char>::size_type)level * 2, ' ')
                   << "[" << emitOffsets(type) << "] " << info.name << " : ";
          if (type.isScalar()) {
            ostream_ << toString(type.kind()) << "<"
                     << toString(
                            type.asScalar().scalarDescriptor().scalarKind())
                     << ">" << std::endl;
          } else {
            ostream_ << toString(type.kind()) << std::endl;
          }
        }
      });
}

void AlphaDumpLib::emitStripes(bool noHeader) {
  Tablet tablet{*pool_, file_.get()};
  TableFormatter formatter(
      ostream_,
      {{"Stripe Id", 11},
       {"Stripe Offset", 15},
       {"Stripe Size", 15},
       {"Row Count", 15}},
      noHeader);
  traverseTablet(*pool_, tablet, std::nullopt, [&](uint32_t stripeIndex) {
    auto sizes = tablet.streamSizes(stripeIndex);
    auto stripeSize = std::accumulate(sizes.begin(), sizes.end(), 0UL);
    formatter.writeRow({
        folly::to<std::string>(stripeIndex),
        folly::to<std::string>(tablet.stripeOffset(stripeIndex)),
        folly::to<std::string>(stripeSize),
        folly::to<std::string>(tablet.stripeRowCount(stripeIndex)),
    });
  });
}

void AlphaDumpLib::emitStreams(
    bool noHeader,
    bool streamLabels,
    std::optional<uint32_t> stripeId) {
  auto tablet = std::make_shared<Tablet>(*pool_, file_.get());

  std::vector<std::tuple<std::string, uint8_t>> fields;
  fields.push_back({"Stripe Id", 11});
  fields.push_back({"Stream Id", 11});
  fields.push_back({"Stream Offset", 13});
  fields.push_back({"Stream Size", 13});
  fields.push_back({"Item Count", 13});
  if (streamLabels) {
    fields.push_back({"Stream Label", 16});
  }
  fields.push_back({"Type", 30});

  TableFormatter formatter(ostream_, fields, noHeader);

  std::optional<StreamLabels> labels{};
  if (streamLabels) {
    VeloxReader reader{*pool_, tablet};
    labels.emplace(reader.schema());
  }

  traverseTablet(
      *pool_,
      *tablet,
      stripeId,
      nullptr /* stripeVisitor */,
      [&](ChunkedStream& stream, uint32_t stripeId, uint32_t streamId) {
        uint32_t itemCount = 0;
        while (stream.hasNext()) {
          auto chunk = stream.nextChunk();
          itemCount += *reinterpret_cast<const uint32_t*>(chunk.data() + 2);
        }
        stream.reset();
        std::vector<std::string> values;
        values.push_back(folly::to<std::string>(stripeId));
        values.push_back(folly::to<std::string>(streamId));
        values.push_back(
            folly::to<std::string>(tablet->streamOffsets(stripeId)[streamId]));
        values.push_back(
            folly::to<std::string>(tablet->streamSizes(stripeId)[streamId]));
        values.push_back(folly::to<std::string>(itemCount));
        if (streamLabels) {
          auto it = values.emplace_back(labels->streamLabel(streamId));
        }
        values.push_back(getStreamInputLabel(stream));
        formatter.writeRow(values);
      });
}

void AlphaDumpLib::emitHistogram(
    bool topLevel,
    bool noHeader,
    std::optional<uint32_t> stripeId) {
  Tablet tablet{*pool_, file_.get()};
  std::map<GroupingKey, size_t, GroupingKeyCompare> encodingHistogram;
  const std::unordered_map<std::string, CompressionType> compressionMap{
      {toString(CompressionType::Uncompressed), CompressionType::Uncompressed},
      {toString(CompressionType::Zstd), CompressionType::Zstd},
      {toString(CompressionType::Zstrong), CompressionType::Zstrong},
  };
  traverseTablet(
      *pool_,
      tablet,
      stripeId,
      nullptr,
      [&](ChunkedStream& stream, auto /*stripeIndex*/, auto /*streamIndex*/) {
        while (stream.hasNext()) {
          traverseEncodings(
              stream.nextChunk(),
              [&](EncodingType encodingType,
                  DataType dataType,
                  uint32_t level,
                  uint32_t /* index */,
                  std::string /*nestedEncodingName*/,
                  std::unordered_map<EncodingPropertyType, EncodingProperty>
                      properties) {
                GroupingKey key{
                    .encodingType = encodingType, .dataType = dataType};
                const auto& compression =
                    properties.find(EncodingPropertyType::Compression);
                if (compression != properties.end()) {
                  key.compressinType =
                      compressionMap.at(compression->second.value);
                }
                ++encodingHistogram[key];
                return !(topLevel && level == 1);
              });
        }
      });

  TableFormatter formatter(
      ostream_,
      {{"Encoding Type", 17},
       {"Data Type", 13},
       {"Compression", 15},
       {"Count", 15}},
      noHeader);

  for (auto& [key, value] : encodingHistogram) {
    formatter.writeRow({
        toString(key.encodingType),
        toString(key.dataType),
        key.compressinType ? toString(*key.compressinType) : "",
        folly::to<std::string>(value),
    });
  }
}

void AlphaDumpLib::emitContent(
    uint32_t streamId,
    std::optional<uint32_t> stripeId) {
  Tablet tablet{*pool_, file_.get()};

  uint32_t maxStreamCount;
  bool found = false;
  traverseTablet(*pool_, tablet, stripeId, [&](uint32_t stripeId) {
    maxStreamCount = std::max(maxStreamCount, tablet.streamCount(stripeId));
    if (streamId >= tablet.streamCount(stripeId)) {
      return;
    }

    found = true;

    auto streams = tablet.load(stripeId, std::vector{streamId});

    if (auto& stream = streams[0]) {
      InMemoryChunkedStream chunkedStream{*pool_, std::move(stream)};
      while (chunkedStream.hasNext()) {
        auto encoding =
            EncodingFactory::decode(*pool_, chunkedStream.nextChunk());
        uint32_t totalRows = encoding->rowCount();
        while (totalRows > 0) {
          auto currentReadSize = std::min(kBufferSize, totalRows);
          printScalarType(ostream_, *pool_, *encoding, currentReadSize);
          totalRows -= currentReadSize;
        }
      }
    }
  });

  if (!found) {
    throw folly::ProgramExit(
        -1,
        fmt::format(
            "Stream identifier {} is out of bound. Must be between 0 and {}\n",
            streamId,
            maxStreamCount));
  }
}

void AlphaDumpLib::emitBinary(
    std::function<std::unique_ptr<std::ostream>()> outputFactory,
    uint32_t streamId,
    uint32_t stripeId) {
  Tablet tablet{*pool_, file_.get()};
  if (streamId >= tablet.streamCount(stripeId)) {
    throw folly::ProgramExit(
        -1,
        fmt::format(
            "Stream identifier {} is out of bound. Must be between 0 and {}\n",
            streamId,
            tablet.streamCount(stripeId)));
  }

  auto streams = tablet.load(stripeId, std::vector{streamId});

  if (auto& stream = streams[0]) {
    auto output = outputFactory();
    output->write(stream->getStream().data(), stream->getStream().size());
    output->flush();
  }
}

void traverseEncodingLayout(
    const std::optional<EncodingLayout>& node,
    const std::optional<EncodingLayout>& parentNode,
    uint32_t& nodeId,
    uint32_t parentId,
    uint32_t level,
    uint8_t childIndex,
    const std::function<void(
        const std::optional<EncodingLayout>&,
        const std::optional<EncodingLayout>&,
        uint32_t,
        uint32_t,
        uint32_t,
        uint8_t)>& visitor) {
  auto currentNodeId = nodeId;
  visitor(node, parentNode, currentNodeId, parentId, level, childIndex);

  if (node.has_value()) {
    for (int i = 0; i < node->childrenCount(); ++i) {
      traverseEncodingLayout(
          node->child(i), node, ++nodeId, currentNodeId, level + 1, i, visitor);
    }
  }
}

void traverseEncodingLayoutTree(
    const EncodingLayoutTree& node,
    const EncodingLayoutTree& parentNode,
    uint32_t& nodeId,
    uint32_t parentId,
    uint32_t level,
    uint8_t childIndex,
    const std::function<void(
        const EncodingLayoutTree&,
        const EncodingLayoutTree&,
        uint32_t,
        uint32_t,
        uint32_t,
        uint8_t)>& visitor) {
  auto currentNodeId = nodeId;
  visitor(node, parentNode, currentNodeId, parentId, level, childIndex);

  for (int i = 0; i < node.childrenCount(); ++i) {
    traverseEncodingLayoutTree(
        node.child(i), node, ++nodeId, currentNodeId, level + 1, i, visitor);
  }
}

std::string getEncodingLayoutLabel(
    const std::optional<alpha::EncodingLayout>& root) {
  std::string label;
  uint32_t currentLevel = 0;
  std::unordered_map<alpha::EncodingType, std::vector<std::string>>
      identifierNames{
          {alpha::EncodingType::Dictionary, {"Alphabet", "Indices"}},
          {alpha::EncodingType::MainlyConstant, {"IsCommon", "OtherValues"}},
          {alpha::EncodingType::Nullable, {"Data", "Nulls"}},
          {alpha::EncodingType::RLE, {"RunLengths", "RunValues"}},
          {alpha::EncodingType::SparseBool, {"Indices"}},
          {alpha::EncodingType::Trivial, {"Lengths"}},
      };

  auto getIdentifierName = [&](alpha::EncodingType encodingType,
                               uint8_t identifier) {
    auto it = identifierNames.find(encodingType);
    LOG(INFO) << (it == identifierNames.end()) << ", "
              << (it != identifierNames.end()
                      ? (int)(identifier >= it->second.size())
                      : -1);
    return it == identifierNames.end() || identifier >= it->second.size()
        ? "Unknown"
        : it->second[identifier];
  };

  uint32_t id = 0;
  traverseEncodingLayout(
      root,
      root,
      id,
      id,
      0,
      (uint8_t)0,
      [&](const std::optional<alpha::EncodingLayout>& node,
          const std::optional<alpha::EncodingLayout>& parentNode,
          uint32_t /* nodeId */,
          uint32_t /* parentId */,
          uint32_t level,
          uint8_t identifier) {
        if (!node.has_value()) {
          label += "N/A";
          return true;
        }

        if (level > currentLevel) {
          label += "[" +
              getIdentifierName(parentNode->encodingType(), identifier) + ":";

        } else if (level < currentLevel) {
          label += "]";
        }

        if (identifier > 0) {
          label += "," +
              getIdentifierName(parentNode->encodingType(), identifier) + ":";
        }

        currentLevel = level;

        label += toString(node->encodingType()) + "{" +
            toString(node->compressionType()) + "}";

        return true;
      });

  while (currentLevel-- > 0) {
    label += "]";
  }

  return label;
}

void AlphaDumpLib::emitLayout(bool noHeader, bool compressed) {
  auto size = file_->size();
  std::string buffer;
  buffer.resize(size);
  file_->pread(0, size, buffer.data());
  if (compressed) {
    std::string uncompressed;
    strings::zstdDecompress(buffer, &uncompressed);
    buffer = std::move(uncompressed);
  }

  auto layout = alpha::EncodingLayoutTree::create(buffer);

  TableFormatter formatter(
      ostream_,
      {
          {"Node Id", 11},
          {"Parent Id", 11},
          {"Node Type", 15},
          {"Node Name", 17},
          {"Encoding Layout", 20},
      },
      noHeader);

  uint32_t id = 0;
  traverseEncodingLayoutTree(
      layout,
      layout,
      id,
      id,
      0,
      0,
      [&](const EncodingLayoutTree& node,
          const EncodingLayoutTree& /* parentNode */,
          uint32_t nodeId,
          uint32_t parentId,
          uint32_t /* level */,
          uint8_t /* identifier */) {
        auto identifiers = node.encodingLayoutIdentifiers();
        std::sort(identifiers.begin(), identifiers.end());

        std::string encodingLayout;
        for (auto identifier : identifiers) {
          if (!encodingLayout.empty()) {
            encodingLayout += "|";
          }
          encodingLayout += folly::to<std::string>(identifier) + ":" +
              getEncodingLayoutLabel(*node.encodingLayout(identifier));
        }

        formatter.writeRow(
            {folly::to<std::string>(nodeId),
             folly::to<std::string>(parentId),
             toString(node.schemaKind()),
             std::string(node.name()),
             encodingLayout});
      });
}

} // namespace facebook::alpha::tools
