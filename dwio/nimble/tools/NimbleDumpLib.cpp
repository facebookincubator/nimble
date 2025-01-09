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
#include <algorithm>
#include <locale>
#include <numeric>
#include <ostream>
#include <tuple>
#include <utility>

#include "common/strings/Zstd.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/encodings/tests/TestUtils.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/tools/EncodingUtilities.h"
#include "dwio/nimble/tools/NimbleDumpLib.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/StatsGenerated.h"
#include "dwio/nimble/velox/VeloxReader.h"
#include "folly/cli/NestedCommandLineApp.h"

namespace facebook::nimble::tools {
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

struct GroupingKeyHash {
  size_t operator()(const GroupingKey& key) const {
    size_t h1 = std::hash<EncodingType>()(key.encodingType);
    size_t h2 = std::hash<DataType>()(key.dataType);
    size_t h3 = std::hash<std::optional<CompressionType>>()(key.compressinType);
    return h1 ^ (h2 << 1) ^ (h3 << 2);
  }
};

struct GroupingKeyEqual {
  bool operator()(const GroupingKey& lhs, const GroupingKey& rhs) const {
    return lhs.encodingType == rhs.encodingType &&
        lhs.dataType == rhs.dataType &&
        lhs.compressinType == rhs.compressinType;
  }
};

struct EncodingHistogramValue {
  size_t count;
  size_t bytes;
};

struct HistogramRowCompare {
  size_t operator()(
      const std::unordered_map<GroupingKey, EncodingHistogramValue>::
          const_iterator& lhs,
      const std::unordered_map<GroupingKey, EncodingHistogramValue>::
          const_iterator& rhs) const {
    const auto lhsEncoding = lhs->first.encodingType;
    const auto rhsEncoding = rhs->first.encodingType;
    const auto lhsSize = lhs->second.bytes;
    const auto rhsSize = rhs->second.bytes;
    if (lhsEncoding != rhsEncoding) {
      return lhsEncoding < rhsEncoding;
    } else {
      return lhsSize > rhsSize;
    }
  }
};

enum class Alignment {
  Left,
  Right,
};

class TableFormatter {
 public:
  TableFormatter(
      std::ostream& ostream,
      std::vector<std::tuple<
          std::string /* Title */,
          uint8_t /* Width */,
          Alignment /* Horizontal Alignment */
          >> fields,
      bool noHeader = false)
      : ostream_{ostream}, fields_{std::move(fields)} {
    if (!noHeader) {
      ostream << YELLOW;
      for (const auto& field : fields_) {
        ostream << (std::get<2>(field) == Alignment::Right ? std::right
                                                           : std::left)
                << std::setw(std::get<1>(field) + 2) << std::get<0>(field);
      }
      ostream << RESET_COLOR << std::endl;
    }
  }

  void writeRow(const std::vector<std::string>& values) {
    assert(values.size() == fields_.size());
    for (auto i = 0; i < values.size(); ++i) {
      ostream_ << (std::get<2>(fields_[i]) == Alignment::Right ? std::right
                                                               : std::left)
               << std::setw(std::get<1>(fields_[i]) + 2) << values[i];
    }
    ostream_ << std::endl;
  }

 private:
  std::ostream& ostream_;
  std::vector<std::tuple<
      std::string /* Title */,
      uint8_t /* Width */,
      Alignment /* Horizontal Alignment */
      >>
      fields_;
};

void traverseTablet(
    velox::memory::MemoryPool& memoryPool,
    const TabletReader& tabletReader,
    std::optional<int32_t> stripeIndex,
    std::function<void(uint32_t /* stripeId */)> stripeVisitor = nullptr,
    std::function<void(
        ChunkedStream& /*stream*/,
        uint32_t /*stripeId*/,
        uint32_t /* streamId*/)> streamVisitor = nullptr) {
  uint32_t startStripe = stripeIndex ? *stripeIndex : 0;
  uint32_t endStripe =
      stripeIndex ? *stripeIndex : tabletReader.stripeCount() - 1;
  for (uint32_t i = startStripe; i <= endStripe; ++i) {
    if (stripeVisitor) {
      stripeVisitor(i);
    }
    if (streamVisitor) {
      auto stripeIdentifier = tabletReader.getStripeIdentifier(i);
      std::vector<uint32_t> streamIdentifiers(
          tabletReader.streamCount(stripeIdentifier));
      std::iota(streamIdentifiers.begin(), streamIdentifiers.end(), 0);
      auto streams = tabletReader.load(
          stripeIdentifier,
          {streamIdentifiers.cbegin(), streamIdentifiers.cend()});
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
  nimble::Vector<T> buffer(&pool);
  nimble::Vector<char> nulls(&pool);
  buffer.resize(rowCount);
  nulls.resize((nimble::FixedBitArray::bufferSize(rowCount, 1)));
  nulls.zero_out();
  if (stream.isNullable()) {
    stream.materializeNullable(
        rowCount, buffer.data(), [&]() { return nulls.data(); });
  } else {
    stream.materialize(rowCount, buffer.data());
    nulls.fill(0xff);
  }
  for (uint32_t i = 0; i < rowCount; ++i) {
    if (stream.isNullable() && nimble::bits::getBit(i, nulls.data()) == 0) {
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
      NIMBLE_UNREACHABLE(
          fmt::format("Undefined type for stream: {}", stream.dataType()));
    }
  }
}

template <typename T>
auto commaSeparated(T value) {
  return fmt::format(std::locale("en_US.UTF-8"), "{:L}", value);
}

} // namespace

NimbleDumpLib::NimbleDumpLib(std::ostream& ostream, const std::string& file)
    : pool_{velox::memory::deprecatedAddDefaultLeafMemoryPool()},
      file_{dwio::file_system::FileSystem::openForRead(
          file,
          dwio::common::request::AccessDescriptorBuilder()
              .withClientId("nimble_dump")
              .build())},
      ostream_{ostream} {}

void NimbleDumpLib::emitInfo() {
  std::vector<std::string> preloadedOptionalSections = {
      std::string(kStatsSection)};
  auto tablet = std::make_shared<TabletReader>(
      *pool_, file_.get(), preloadedOptionalSections);
  ostream_ << CYAN << "Nimble File " << RESET_COLOR << "Version "
           << tablet->majorVersion() << "." << tablet->minorVersion()
           << std::endl;
  ostream_ << "File Size: " << commaSeparated(tablet->fileSize()) << std::endl;
  ostream_ << "Checksum: " << tablet->checksum() << " ["
           << nimble::toString(tablet->checksumType()) << "]" << std::endl;
  ostream_ << "Footer Compression: " << tablet->footerCompressionType()
           << std::endl;
  ostream_ << "Footer Size: " << commaSeparated(tablet->footerSize())
           << std::endl;
  ostream_ << "Stripe Count: " << commaSeparated(tablet->stripeCount())
           << std::endl;
  ostream_ << "Row Count: " << commaSeparated(tablet->tabletRowCount())
           << std::endl;

  VeloxReader reader{*pool_, tablet};

  auto statsSection =
      tablet.get()->loadOptionalSection(preloadedOptionalSections[0]);
  ostream_ << "Raw Data Size: ";
  if (statsSection.has_value()) {
    auto rawSize = flatbuffers::GetRoot<nimble::serialization::Stats>(
                       statsSection->content().data())
                       ->raw_size();
    ostream_ << rawSize << std::endl;
  } else {
    ostream_ << "N/A" << std::endl;
  }

  auto& metadata = reader.metadata();
  if (!metadata.empty()) {
    ostream_ << "Metadata:";
    for (const auto& pair : metadata) {
      ostream_ << std::endl << "  " << pair.first << ": " << pair.second;
    }
  }
  ostream_ << std::endl;
}

void NimbleDumpLib::emitSchema(bool collapseFlatMap) {
  auto tablet = std::make_shared<TabletReader>(*pool_, file_.get());
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
      case Kind::SlidingWindowMap: {
        offsets = "o:" +
            folly::to<std::string>(
                      type.asSlidingWindowMap().offsetsDescriptor().offset()) +
            ",l:" +
            folly::to<std::string>(
                      type.asSlidingWindowMap().lengthsDescriptor().offset());
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

void NimbleDumpLib::emitStripes(bool noHeader) {
  TabletReader tabletReader{*pool_, file_.get()};
  TableFormatter formatter(
      ostream_,
      {{"Stripe Id", 7, Alignment::Left},
       {"Stripe Offset", 15, Alignment::Right},
       {"Stripe Size", 15, Alignment::Right},
       {"Row Count", 10, Alignment::Right}},
      noHeader);
  traverseTablet(*pool_, tabletReader, std::nullopt, [&](uint32_t stripeIndex) {
    auto stripeIdentifier = tabletReader.getStripeIdentifier(stripeIndex);
    auto sizes = tabletReader.streamSizes(stripeIdentifier);
    auto stripeSize = std::accumulate(sizes.begin(), sizes.end(), 0UL);
    formatter.writeRow({
        folly::to<std::string>(stripeIndex),
        commaSeparated(tabletReader.stripeOffset(stripeIndex)),
        commaSeparated(stripeSize),
        commaSeparated(tabletReader.stripeRowCount(stripeIndex)),
    });
  });
}

void NimbleDumpLib::emitStreams(
    bool noHeader,
    bool showStreamLabels,
    bool showStreamRawSize,
    std::optional<uint32_t> stripeId) {
  auto tabletReader = std::make_shared<TabletReader>(*pool_, file_.get());

  std::vector<std::tuple<std::string, uint8_t, Alignment>> fields;
  fields.push_back({"Stripe Id", 11, Alignment::Left});
  fields.push_back({"Stream Id", 11, Alignment::Left});
  fields.push_back({"Stream Offset", 13, Alignment::Left});
  fields.push_back({"Stream Size", 13, Alignment::Left});
  if (showStreamRawSize) {
    fields.push_back({"Raw Stream Size", 16, Alignment::Left});
  }
  fields.push_back({"Item Count", 13, Alignment::Left});
  if (showStreamLabels) {
    fields.push_back({"Stream Label", 16, Alignment::Left});
  }
  fields.push_back({"Type", 30, Alignment::Left});

  TableFormatter formatter(ostream_, fields, noHeader);

  std::optional<StreamLabels> labels{};
  if (showStreamLabels) {
    VeloxReader reader{*pool_, tabletReader};
    labels.emplace(reader.schema());
  }

  traverseTablet(
      *pool_,
      *tabletReader,
      stripeId,
      nullptr /* stripeVisitor */,
      [&](ChunkedStream& stream, uint32_t stripeId, uint32_t streamId) {
        auto stripeIdentifier = tabletReader->getStripeIdentifier(stripeId);
        uint32_t itemCount = 0;
        uint64_t rawStreamSize = 0;
        while (stream.hasNext()) {
          auto chunk = stream.nextChunk();
          itemCount += *reinterpret_cast<const uint32_t*>(chunk.data() + 2);
          if (showStreamRawSize) {
            rawStreamSize +=
                nimble::test::TestUtils::getRawDataSize(*pool_, chunk);
          }
        }

        stream.reset();
        std::vector<std::string> values;
        values.push_back(folly::to<std::string>(stripeId));
        values.push_back(folly::to<std::string>(streamId));
        values.push_back(folly::to<std::string>(
            tabletReader->streamOffsets(stripeIdentifier)[streamId]));
        values.push_back(folly::to<std::string>(
            tabletReader->streamSizes(stripeIdentifier)[streamId]));
        if (showStreamRawSize) {
          values.push_back(folly::to<std::string>(rawStreamSize));
        }
        values.push_back(folly::to<std::string>(itemCount));
        if (showStreamLabels) {
          auto it = values.emplace_back(labels->streamLabel(streamId));
        }
        values.push_back(getStreamInputLabel(stream));
        formatter.writeRow(values);
      });
}

void NimbleDumpLib::emitHistogram(
    bool topLevel,
    bool noHeader,
    std::optional<uint32_t> stripeId) {
  TabletReader tabletReader{*pool_, file_.get()};
  std::unordered_map<
      GroupingKey,
      EncodingHistogramValue,
      GroupingKeyHash,
      GroupingKeyEqual>
      encodingHistogram;
  const std::unordered_map<std::string, CompressionType> compressionMap{
      {toString(CompressionType::Uncompressed), CompressionType::Uncompressed},
      {toString(CompressionType::Zstd), CompressionType::Zstd},
      {toString(CompressionType::MetaInternal), CompressionType::MetaInternal},
  };
  traverseTablet(
      *pool_,
      tabletReader,
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
                auto& value = encodingHistogram[key];
                ++value.count;

                const auto& encodedSize =
                    properties.find(EncodingPropertyType::EncodedSize);
                if (encodedSize != properties.end()) {
                  value.bytes += folly::to<uint32_t>(encodedSize->second.value);
                }

                return !(topLevel && level == 1);
              });
        }
      });

  TableFormatter formatter(
      ostream_,
      {{"Encoding Type", 17, Alignment::Left},
       {"Data Type", 13, Alignment::Left},
       {"Compression", 15, Alignment::Left},
       {"Instance Count", 15, Alignment::Right},
       {"Storage Bytes", 15, Alignment::Right},
       {"Storage %", 10, Alignment::Right}},
      noHeader);

  std::vector<
      std::unordered_map<GroupingKey, EncodingHistogramValue>::const_iterator>
      rows;
  for (auto it = encodingHistogram.begin(); it != encodingHistogram.end();
       ++it) {
    rows.push_back(it);
  }
  std::sort(rows.begin(), rows.end(), HistogramRowCompare{});
  const auto fileSize = tabletReader.fileSize();

  for (const auto& it : rows) {
    formatter.writeRow({
        toString(it->first.encodingType),
        toString(it->first.dataType),
        it->first.compressinType ? toString(*it->first.compressinType) : "",
        commaSeparated(it->second.count),
        commaSeparated(it->second.bytes),
        fmt::format("{:.2f}", it->second.bytes * 100.0 / fileSize),
    });
  }
}

void NimbleDumpLib::emitContent(
    uint32_t streamId,
    std::optional<uint32_t> stripeId) {
  TabletReader tabletReader{*pool_, file_.get()};

  uint32_t maxStreamCount;
  bool found = false;
  traverseTablet(*pool_, tabletReader, stripeId, [&](uint32_t stripeId) {
    auto stripeIdentifier = tabletReader.getStripeIdentifier(stripeId);
    maxStreamCount =
        std::max(maxStreamCount, tabletReader.streamCount(stripeIdentifier));
    if (streamId >= tabletReader.streamCount(stripeIdentifier)) {
      return;
    }

    found = true;

    auto streams = tabletReader.load(stripeIdentifier, std::vector{streamId});

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

void NimbleDumpLib::emitBinary(
    std::function<std::unique_ptr<std::ostream>()> outputFactory,
    uint32_t streamId,
    uint32_t stripeId) {
  TabletReader tabletReader{*pool_, file_.get()};
  auto stripeIdentifier = tabletReader.getStripeIdentifier(stripeId);
  if (streamId >= tabletReader.streamCount(stripeIdentifier)) {
    throw folly::ProgramExit(
        -1,
        fmt::format(
            "Stream identifier {} is out of bound. Must be between 0 and {}\n",
            streamId,
            tabletReader.streamCount(stripeIdentifier)));
  }

  auto streams = tabletReader.load(stripeIdentifier, std::vector{streamId});

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
    const std::optional<nimble::EncodingLayout>& root) {
  std::string label;
  uint32_t currentLevel = 0;
  std::unordered_map<nimble::EncodingType, std::vector<std::string>>
      identifierNames{
          {nimble::EncodingType::Dictionary, {"Alphabet", "Indices"}},
          {nimble::EncodingType::MainlyConstant, {"IsCommon", "OtherValues"}},
          {nimble::EncodingType::Nullable, {"Data", "Nulls"}},
          {nimble::EncodingType::RLE, {"RunLengths", "RunValues"}},
          {nimble::EncodingType::SparseBool, {"Indices"}},
          {nimble::EncodingType::Trivial, {"Lengths"}},
      };

  auto getIdentifierName = [&](nimble::EncodingType encodingType,
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
      [&](const std::optional<nimble::EncodingLayout>& node,
          const std::optional<nimble::EncodingLayout>& parentNode,
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

void NimbleDumpLib::emitLayout(bool noHeader, bool compressed) {
  auto size = file_->size();
  std::string buffer;
  buffer.resize(size);
  file_->pread(0, size, buffer.data());
  if (compressed) {
    std::string uncompressed;
    strings::zstdDecompress(buffer, &uncompressed);
    buffer = std::move(uncompressed);
  }

  auto layout = nimble::EncodingLayoutTree::create(buffer);

  TableFormatter formatter(
      ostream_,
      {
          {"Node Id", 11, Alignment::Left},
          {"Parent Id", 11, Alignment::Left},
          {"Node Type", 15, Alignment::Left},
          {"Node Name", 17, Alignment::Left},
          {"Encoding Layout", 20, Alignment::Left},
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

} // namespace facebook::nimble::tools
