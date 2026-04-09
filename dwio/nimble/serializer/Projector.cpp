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

#include "dwio/nimble/serializer/Projector.h"

#include <algorithm>
#include <numeric>

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "dwio/nimble/velox/SchemaUtils.h"
#include "folly/io/Cursor.h"
#include "velox/type/Type.h"

namespace facebook::nimble::serde {

namespace {

// Lightweight adapter for writing small sections (header, trailer) directly
// into an IOBuf. Satisfies the size()/resize()/data() interface required by
// detail::extend/writeHeader/writeTrailer. Avoids the std::string → IOBuf
// copy that would occur if writing into a temporary std::string first.
class IOBufSection {
 public:
  explicit IOBufSection(size_t initialCapacity)
      : buf_(folly::IOBuf::create(initialCapacity)) {}

  size_t size() const {
    return size_;
  }

  void resize(size_t newSize) {
    if (newSize > buf_->capacity()) {
      auto newBuf =
          folly::IOBuf::create(std::max(newSize, buf_->capacity() * 2));
      if (size_ > 0) {
        std::memcpy(newBuf->writableData(), buf_->data(), size_);
      }
      buf_ = std::move(newBuf);
    }
    size_ = newSize;
  }

  char* data() {
    return reinterpret_cast<char*>(buf_->writableData());
  }

  // Finalizes the IOBuf by setting its length and returns it.
  std::unique_ptr<folly::IOBuf> build() && {
    buf_->append(size_);
    return std::move(buf_);
  }

 private:
  std::unique_ptr<folly::IOBuf> buf_;
  size_t size_{0};
};

// Reads a varint32 from a Cursor, advancing past the encoded bytes.
uint32_t readVarint32(folly::io::Cursor& cursor) {
  uint32_t value = 0;
  uint32_t shift = 0;
  while (true) {
    auto byte = cursor.read<uint8_t>();
    value |= static_cast<uint32_t>(byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return value;
    }
    shift += 7;
  }
}

// Forward declaration for recursive calls from per-type helpers.
std::shared_ptr<const Type> updateColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::Type& projectType);

// Updates row column names via positional matching with velox RowType.
std::shared_ptr<const Type> updateRowColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::RowType& veloxRow) {
  const auto& nimbleRow = inputType->asRow();
  std::vector<std::string> newNames;
  std::vector<std::shared_ptr<const Type>> newChildren;
  newNames.reserve(nimbleRow.childrenCount());
  newChildren.reserve(nimbleRow.childrenCount());

  bool changed{false};
  const auto minChildren =
      std::min<size_t>(veloxRow.size(), nimbleRow.childrenCount());
  for (size_t i = 0; i < minChildren; ++i) {
    newNames.emplace_back(veloxRow.nameOf(i));
    changed |= (newNames.back() != nimbleRow.nameAt(i));
    auto newChild =
        updateColumnNames(nimbleRow.childAt(i), *veloxRow.childAt(i));
    changed |= (newChild != nimbleRow.childAt(i));
    newChildren.emplace_back(std::move(newChild));
  }

  if (!changed) {
    return inputType;
  }

  // Keep remaining children as-is (file has more columns than table).
  newNames.insert(
      newNames.end(),
      nimbleRow.names().begin() + minChildren,
      nimbleRow.names().end());
  newChildren.insert(
      newChildren.end(),
      nimbleRow.children().begin() + minChildren,
      nimbleRow.children().end());
  return std::make_shared<RowType>(
      nimbleRow.nullsDescriptor(), std::move(newNames), std::move(newChildren));
}

// Updates column names within array elements.
// Handles both Array and ArrayWithOffsets nimble types.
std::shared_ptr<const Type> updateArrayColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::ArrayType& veloxArray) {
  if (inputType->kind() == Kind::ArrayWithOffsets) {
    const auto& array = inputType->asArrayWithOffsets();
    auto newElements =
        updateColumnNames(array.elements(), *veloxArray.elementType());
    if (newElements == array.elements()) {
      return inputType;
    }
    return std::make_shared<ArrayWithOffsetsType>(
        array.offsetsDescriptor(),
        array.lengthsDescriptor(),
        std::move(newElements));
  }
  const auto& array = inputType->asArray();
  auto newElements =
      updateColumnNames(array.elements(), *veloxArray.elementType());
  if (newElements == array.elements()) {
    return inputType;
  }
  return std::make_shared<ArrayType>(
      array.lengthsDescriptor(), std::move(newElements));
}

// Updates column names within FlatMap value types.
// FlatMap keys are data-determined, not renamed.
std::shared_ptr<const Type> updateFlatMapColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::MapType& veloxMap) {
  const auto& flatMap = inputType->asFlatMap();
  std::vector<std::shared_ptr<const Type>> newChildren;
  newChildren.reserve(flatMap.childrenCount());
  bool changed{false};
  for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
    auto newChild =
        updateColumnNames(flatMap.childAt(i), *veloxMap.valueType());
    changed |= (newChild != flatMap.childAt(i));
    newChildren.emplace_back(std::move(newChild));
  }
  if (!changed) {
    return inputType;
  }
  // Clone names and inMapDescriptors.
  std::vector<std::string> newNames;
  newNames.reserve(flatMap.childrenCount());
  std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors;
  inMapDescriptors.reserve(flatMap.childrenCount());
  for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
    newNames.emplace_back(flatMap.nameAt(i));
    const auto& desc = flatMap.inMapDescriptorAt(i);
    inMapDescriptors.emplace_back(
        std::make_unique<StreamDescriptor>(desc.offset(), desc.scalarKind()));
  }
  return std::make_shared<FlatMapType>(
      flatMap.nullsDescriptor(),
      flatMap.keyScalarKind(),
      std::move(newNames),
      std::move(inMapDescriptors),
      std::move(newChildren));
}

// Updates column names within Map/SlidingWindowMap key and value types.
std::shared_ptr<const Type> updateMapColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::MapType& veloxMap) {
  switch (inputType->kind()) {
    case Kind::SlidingWindowMap: {
      const auto& map = inputType->asSlidingWindowMap();
      auto newKeys = updateColumnNames(map.keys(), *veloxMap.keyType());
      auto newValues = updateColumnNames(map.values(), *veloxMap.valueType());
      if (newKeys == map.keys() && newValues == map.values()) {
        return inputType;
      }
      return std::make_shared<SlidingWindowMapType>(
          map.offsetsDescriptor(),
          map.lengthsDescriptor(),
          std::move(newKeys),
          std::move(newValues));
    }
    case Kind::Map: {
      const auto& map = inputType->asMap();
      auto newKeys = updateColumnNames(map.keys(), *veloxMap.keyType());
      auto newValues = updateColumnNames(map.values(), *veloxMap.valueType());
      if (newKeys == map.keys() && newValues == map.values()) {
        return inputType;
      }
      return std::make_shared<MapType>(
          map.lengthsDescriptor(), std::move(newKeys), std::move(newValues));
    }
    default:
      NIMBLE_UNREACHABLE(
          "updateMapColumnNames called with unsupported kind: {}",
          inputType->kind());
  }
}

// Updates column names in nimble schema to match projectType (velox) using
// positional matching. Same algorithm as Reader::updateColumnNames but across
// velox/nimble type trees.
std::shared_ptr<const Type> updateColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::Type& projectType) {
  switch (projectType.kind()) {
    case velox::TypeKind::ROW:
      return updateRowColumnNames(inputType, projectType.asRow());
    case velox::TypeKind::ARRAY:
      return updateArrayColumnNames(inputType, projectType.asArray());
    case velox::TypeKind::MAP:
      if (inputType->isFlatMap()) {
        return updateFlatMapColumnNames(inputType, projectType.asMap());
      }
      return updateMapColumnNames(inputType, projectType.asMap());
    default:
      return inputType;
  }
}

} // namespace

Projector::Projector(
    std::shared_ptr<const Type> inputSchema,
    const std::vector<Subfield>& projectSubfields,
    velox::memory::MemoryPool* pool,
    Options options)
    : pool_(pool),
      options_(std::move(options)),
      streamSizesEncodingBuffer_(*pool, /*initialChunkSize=*/4096),
      inputSchema_(std::move(inputSchema)) {
  NIMBLE_CHECK_NOT_NULL(pool_, "Memory pool cannot be null");
  NIMBLE_CHECK_NOT_NULL(inputSchema_, "Input schema cannot be null");
  NIMBLE_CHECK(
      inputSchema_->isRow(),
      "Input schema must be a RowType, got: {}",
      inputSchema_->kind());
  NIMBLE_CHECK(!projectSubfields.empty(), "Must project at least one subfield");
  NIMBLE_CHECK(
      isCompactFormat(options_.projectVersion),
      "Projection output version must be kCompact or kCompactRaw, got: {}",
      options_.projectVersion);

  // Update inputSchema_ with projectType names for schema evolution.
  if (options_.projectType) {
    inputSchema_ = updateColumnNames(inputSchema_, *options_.projectType);
  }

  for (const auto& subfield : projectSubfields) {
    NIMBLE_CHECK(subfield.valid(), "Invalid subfield: {}", subfield.toString());
  }

  projectedSchema_ = buildProjectedNimbleType(
      inputSchema_.get(), projectSubfields, inputStreamIndices_);

  inputStreamsSorted_ =
      std::is_sorted(inputStreamIndices_.begin(), inputStreamIndices_.end());

  if (!inputStreamsSorted_) {
    sortedStreamMappings_.reserve(inputStreamIndices_.size());
    for (size_t i = 0; i < inputStreamIndices_.size(); ++i) {
      sortedStreamMappings_.push_back({inputStreamIndices_[i], i});
    }
    std::sort(
        sortedStreamMappings_.begin(),
        sortedStreamMappings_.end(),
        [](const auto& lhs, const auto& rhs) {
          return lhs.inputStreamIdx < rhs.inputStreamIdx;
        });
  }
}

namespace {

// Validates the input version header is kCompact or kCompactRaw.
SerializationVersion getAndValidateInputVersion(const folly::IOBuf& input) {
  const auto version = static_cast<SerializationVersion>(*input.data());
  NIMBLE_CHECK(
      isCompactFormat(version),
      "Input must be kCompact or kCompactRaw format, got: {}",
      version);
  return version;
}

} // namespace

// Projects selected streams from a contiguous IOBuf into the output chain.
// Streams are output in the order of selectedIndices (output stream order),
// which may differ from sorted input order for interleaved schemas (e.g.,
// multiple FlatMap columns). Merges adjacent output entries that are contiguous
// in the input into a single zero-copy IOBuf clone to minimize IOBuf
// allocations. The input IOBuf must outlive the output chain.
// static
std::vector<uint32_t> Projector::projectStreamsContiguousUnsorted(
    const folly::IOBuf& input,
    size_t dataOffset,
    const std::vector<uint32_t>& streamSizes,
    const std::vector<uint32_t>& selectedIndices,
    std::unique_ptr<folly::IOBuf>& output) {
  std::vector<uint32_t> outputSizes(selectedIndices.size(), 0);

  // Pre-compute byte offset for each input stream.
  std::vector<size_t> streamOffsets(streamSizes.size());
  size_t streamOffset = dataOffset;
  for (size_t i = 0; i < streamSizes.size(); ++i) {
    streamOffsets[i] = streamOffset;
    streamOffset += streamSizes[i];
  }

  // Track the start offset and byte count of a contiguous run.
  size_t runStart = 0;
  size_t numRunBytes = 0;

  auto flushRun = [&]() {
    if (numRunBytes == 0) {
      return;
    }
    // Zero-copy: clone the input and trim to the run's sub-range.
    auto buf = input.cloneOne();
    buf->trimStart(runStart);
    buf->trimEnd(buf->length() - numRunBytes);
    output->appendToChain(std::move(buf));
    numRunBytes = 0;
  };

  // Output streams in output order, merging entries that are contiguous in
  // input.
  for (size_t i = 0; i < selectedIndices.size(); ++i) {
    const auto streamIdx = selectedIndices[i];
    // Streams beyond streamSizes are treated as empty. This happens when the
    // serializer omits constant in-map boolean streams (all-true/all-false),
    // making the serialized stream count smaller than the schema's max offset.
    if (streamIdx >= streamSizes.size() || streamSizes[streamIdx] == 0) {
      continue;
    }
    outputSizes[i] = streamSizes[streamIdx];
    if (numRunBytes > 0 && streamOffsets[streamIdx] == runStart + numRunBytes) {
      numRunBytes += streamSizes[streamIdx];
    } else {
      flushRun();
      runStart = streamOffsets[streamIdx];
      numRunBytes = streamSizes[streamIdx];
    }
  }
  flushRun();

  return outputSizes;
}

// Fast path for projectStreamsContiguousUnsorted when inputStreamIndices are
// sorted (no FlatMap key reordering). Avoids pre-computing byte offsets for all
// streams — instead uses a single forward scan tracking the current offset.
// static
std::vector<uint32_t> Projector::projectStreamsContiguousSorted(
    const folly::IOBuf& input,
    size_t dataOffset,
    const std::vector<uint32_t>& streamSizes,
    const std::vector<uint32_t>& selectedIndices,
    std::unique_ptr<folly::IOBuf>& output) {
  std::vector<uint32_t> outputSizes(selectedIndices.size(), 0);

  size_t curOffset = dataOffset;
  size_t nextSelected = 0;

  // Track the start offset and byte count of a contiguous run.
  size_t runStart = 0;
  size_t numRunBytes = 0;

  auto flushRun = [&]() {
    if (numRunBytes == 0) {
      return;
    }
    // Zero-copy: clone the input and trim to the run's sub-range.
    auto buf = input.cloneOne();
    buf->trimStart(runStart);
    buf->trimEnd(buf->length() - numRunBytes);
    output->appendToChain(std::move(buf));
    numRunBytes = 0;
  };

  for (size_t i = 0;
       i < streamSizes.size() && nextSelected < selectedIndices.size();
       ++i) {
    if (i == selectedIndices[nextSelected]) {
      if (streamSizes[i] > 0) {
        outputSizes[nextSelected] = streamSizes[i];
        if (numRunBytes > 0 && curOffset == runStart + numRunBytes) {
          numRunBytes += streamSizes[i];
        } else {
          flushRun();
          runStart = curOffset;
          numRunBytes = streamSizes[i];
        }
      }
      ++nextSelected;
    }
    curOffset += streamSizes[i];
  }
  flushRun();

  return outputSizes;
}

// Fast path for projectStreamsChainedUnsorted when inputStreamIndices are
// sorted (no FlatMap key reordering). Single forward pass with run merging — no
// sorting or reordering needed since input and output order already match.
// static
std::vector<uint32_t> Projector::projectStreamsChainedSorted(
    folly::io::Cursor& cursor,
    const std::vector<uint32_t>& streamSizes,
    const std::vector<uint32_t>& selectedIndices,
    std::unique_ptr<folly::IOBuf>& output) {
  std::vector<uint32_t> outputSizes(selectedIndices.size(), 0);
  size_t nextSelected = 0;

  // Track how many contiguous selected streams we can merge.
  size_t numRunBytes = 0;

  auto flushRun = [&](std::unique_ptr<folly::IOBuf>& out) {
    if (numRunBytes == 0) {
      return;
    }
    std::unique_ptr<folly::IOBuf> buf;
    cursor.clone(buf, numRunBytes);
    out->appendToChain(std::move(buf));
    numRunBytes = 0;
  };

  for (size_t i = 0;
       i < streamSizes.size() && nextSelected < selectedIndices.size();
       ++i) {
    if (i == selectedIndices[nextSelected]) {
      outputSizes[nextSelected] = streamSizes[i];
      if (streamSizes[i] > 0) {
        numRunBytes += streamSizes[i];
      }
      ++nextSelected;
      continue;
    }
    // Not selected — flush any pending run and skip this stream.
    flushRun(output);
    if (streamSizes[i] > 0) {
      cursor.skip(streamSizes[i]);
    }
  }
  flushRun(output);

  return outputSizes;
}

// Projects selected streams from a chained IOBuf via cursor.
// Streams are output in the order of selectedIndices (output stream order).
// Uses a sorted forward pass to extract IOBufs via cursor, merging contiguous
// runs that are also adjacent in output order into single zero-copy clones.
// Non-mergeable streams are extracted individually and reordered in a second
// pass.

// static
std::vector<uint32_t> Projector::projectStreamsChainedUnsorted(
    folly::io::Cursor& cursor,
    const std::vector<uint32_t>& streamSizes,
    const std::vector<StreamMapping>& sortedStreamMappings,
    std::unique_ptr<folly::IOBuf>& output) {
  std::vector<uint32_t> outputSizes(sortedStreamMappings.size(), 0);

  // Holds the extracted IOBuf for one or more merged output streams.
  struct OutputStreamBuffer {
    // Start output stream index. When multiple contiguous input streams are
    // merged, this is the index of the first output stream in the merged run.
    size_t outputStreamIdx;
    std::unique_ptr<folly::IOBuf> buf;
  };

  // Forward pass: extract selected streams via cursor in sorted input order.
  // Merges contiguous input streams that are also adjacent in output order into
  // a single cursor.clone() call, reducing allocations. Each merged run
  // produces one OutputStreamBuffer entry — typically far fewer than the total
  // number of selected streams.
  std::vector<OutputStreamBuffer> outputStreamBufs;
  outputStreamBufs.reserve(sortedStreamMappings.size());
  size_t nextSortedIdx = 0;
  for (size_t i = 0;
       i < streamSizes.size() && nextSortedIdx < sortedStreamMappings.size();) {
    if (i != sortedStreamMappings[nextSortedIdx].inputStreamIdx) {
      if (streamSizes[i] > 0) {
        cursor.skip(streamSizes[i]);
      }
      ++i;
      continue;
    }

    // Found a selected stream. Try to extend a contiguous run: consecutive
    // in input AND adjacent in output order.
    const auto runOutputStreamIdx =
        sortedStreamMappings[nextSortedIdx].outputStreamIdx;
    size_t numRunBytes = 0;
    size_t numRunOutputStreams = 0;
    while (nextSortedIdx + numRunOutputStreams < sortedStreamMappings.size() &&
           sortedStreamMappings[nextSortedIdx + numRunOutputStreams]
                   .inputStreamIdx == i + numRunOutputStreams &&
           i + numRunOutputStreams < streamSizes.size() &&
           sortedStreamMappings[nextSortedIdx + numRunOutputStreams]
                   .outputStreamIdx ==
               runOutputStreamIdx + numRunOutputStreams) {
      const auto inputStreamIdx =
          sortedStreamMappings[nextSortedIdx + numRunOutputStreams]
              .inputStreamIdx;
      outputSizes[runOutputStreamIdx + numRunOutputStreams] =
          streamSizes[inputStreamIdx];
      numRunBytes += streamSizes[inputStreamIdx];
      ++numRunOutputStreams;
    }

    if (numRunBytes > 0) {
      // Zero-copy: clone the merged run.
      std::unique_ptr<folly::IOBuf> buf;
      cursor.clone(buf, numRunBytes);
      outputStreamBufs.push_back({runOutputStreamIdx, std::move(buf)});
    }
    nextSortedIdx += numRunOutputStreams;
    i += numRunOutputStreams;
  }

  // Sort by output stream index and chain in order.
  std::sort(
      outputStreamBufs.begin(),
      outputStreamBufs.end(),
      [](const auto& lhs, const auto& rhs) {
        return lhs.outputStreamIdx < rhs.outputStreamIdx;
      });
  for (auto& streamBuf : outputStreamBufs) {
    output->appendToChain(std::move(streamBuf.buf));
  }
  return outputSizes;
}

folly::IOBuf Projector::buildProjectedOutput(
    const std::vector<uint32_t>& outputStreamSizes,
    std::unique_ptr<folly::IOBuf> output) const {
  IOBufSection trailer(
      detail::estimateTrailerSize(
          options_.projectVersion,
          outputStreamSizes.size(),
          options_.streamSizesEncodingType));
  detail::writeTrailer(
      options_.projectVersion,
      outputStreamSizes,
      options_.streamSizesEncodingType,
      streamSizesEncodingBuffer_,
      trailer);
  output->appendToChain(std::move(trailer).build());
  return std::move(*output);
}

folly::IOBuf Projector::project(const folly::IOBuf& input) const {
  const auto inputVersion = getAndValidateInputVersion(input);

  if (!input.isChained()) {
    return projectContiguous(input, inputVersion);
  }
  return projectChained(input, inputVersion);
}

folly::IOBuf Projector::projectContiguous(
    const folly::IOBuf& input,
    SerializationVersion inputVersion) const {
  const auto* data = reinterpret_cast<const char*>(input.data());
  // Skip version byte (already validated and passed in).
  const auto* pos = data + sizeof(uint8_t);
  const uint32_t rowCount = varint::readVarint32(&pos);

  // Build header: [version byte][varint rowCount].
  IOBufSection header(
      detail::estimateHeaderSize(options_.projectVersion, rowCount));
  detail::writeHeader(header, options_.projectVersion, rowCount);
  auto output = std::move(header).build();

  const auto inputStreamSizes =
      detail::readStreamSizes(input, inputVersion, pool_);

  // Extract selected streams as zero-copy sub-range clones.
  const auto dataOffset = static_cast<size_t>(pos - data);
  auto outputStreamSizes = inputStreamsSorted_
      ? projectStreamsContiguousSorted(
            input, dataOffset, inputStreamSizes, inputStreamIndices_, output)
      : projectStreamsContiguousUnsorted(
            input, dataOffset, inputStreamSizes, inputStreamIndices_, output);

  return buildProjectedOutput(outputStreamSizes, std::move(output));
}

folly::IOBuf Projector::projectChained(
    const folly::IOBuf& input,
    SerializationVersion inputVersion) const {
  folly::io::Cursor cursor(&input);
  // Skip version byte (already validated and passed in).
  cursor.skip(sizeof(uint8_t));
  const uint32_t rowCount = readVarint32(cursor);

  // Build header: [version byte][varint rowCount].
  IOBufSection header(
      detail::estimateHeaderSize(options_.projectVersion, rowCount));
  detail::writeHeader(header, options_.projectVersion, rowCount);
  auto output = std::move(header).build();

  const auto inputStreamSizes =
      detail::readStreamSizes(input, inputVersion, pool_);

  // Extract selected streams as zero-copy clones via cursor.
  auto outputStreamSizes = inputStreamsSorted_
      ? projectStreamsChainedSorted(
            cursor, inputStreamSizes, inputStreamIndices_, output)
      : projectStreamsChainedUnsorted(
            cursor, inputStreamSizes, sortedStreamMappings_, output);

  return buildProjectedOutput(outputStreamSizes, std::move(output));
}

folly::IOBuf Projector::project(std::string_view input) const {
  return project(folly::IOBuf::wrapBufferAsValue(input.data(), input.size()));
}

void Projector::project(std::string_view input, std::string& output) const {
  const auto inputVersion = static_cast<SerializationVersion>(
      *reinterpret_cast<const uint8_t*>(input.data()));
  NIMBLE_CHECK(
      isCompactFormat(inputVersion),
      "Input must be kCompact or kCompactRaw format, got: {}",
      inputVersion);

  const auto* data = input.data();
  const auto* pos = data + sizeof(uint8_t);
  const uint32_t rowCount = varint::readVarint32(&pos);
  const size_t dataOffset = static_cast<size_t>(pos - data);

  // Read input stream sizes from trailer.
  const auto* end = data + input.size();
  const auto inputStreamSizes =
      detail::readStreamSizes(end, inputVersion, pool_);

  // Compute output stream sizes and total data bytes.
  std::vector<uint32_t> outputStreamSizes(inputStreamIndices_.size());
  std::transform(
      inputStreamIndices_.begin(),
      inputStreamIndices_.end(),
      outputStreamSizes.begin(),
      [&](auto idx) -> uint32_t {
        return idx < inputStreamSizes.size() ? inputStreamSizes[idx] : 0;
      });
  const size_t totalDataBytes = std::accumulate(
      outputStreamSizes.begin(), outputStreamSizes.end(), size_t{0});

  // Estimate total output size and pre-allocate.
  const size_t headerSize =
      detail::estimateHeaderSize(options_.projectVersion, rowCount);
  const size_t trailerEstimate = detail::estimateTrailerSize(
      options_.projectVersion,
      outputStreamSizes.size(),
      options_.streamSizesEncodingType);

  output.clear();
  output.reserve(headerSize + totalDataBytes + trailerEstimate);

  // Write header.
  detail::writeHeader(output, options_.projectVersion, rowCount);

  if (inputStreamsSorted_) {
    // Fast path: input indices are sorted, single forward pass with run
    // merging via memcpy.
    size_t curOffset = dataOffset;
    size_t nextSelected = 0;

    size_t runStart = 0;
    size_t numRunBytes = 0;

    for (size_t i = 0; i < inputStreamSizes.size() &&
         nextSelected < inputStreamIndices_.size();
         ++i) {
      if (i == inputStreamIndices_[nextSelected]) {
        if (inputStreamSizes[i] > 0) {
          if (numRunBytes > 0 && curOffset == runStart + numRunBytes) {
            numRunBytes += inputStreamSizes[i];
          } else {
            if (numRunBytes > 0) {
              output.append(data + runStart, numRunBytes);
            }
            runStart = curOffset;
            numRunBytes = inputStreamSizes[i];
          }
        }
        ++nextSelected;
      }
      curOffset += inputStreamSizes[i];
    }
    if (numRunBytes > 0) {
      output.append(data + runStart, numRunBytes);
    }
  } else {
    // Unsorted path: compute input stream offsets, then copy in output order.
    std::vector<size_t> inputOffsets(inputStreamSizes.size());
    std::exclusive_scan(
        inputStreamSizes.begin(),
        inputStreamSizes.end(),
        inputOffsets.begin(),
        dataOffset);
    for (size_t i = 0; i < inputStreamIndices_.size(); ++i) {
      const auto idx = inputStreamIndices_[i];
      if (idx < inputStreamSizes.size() && idx < inputOffsets.size() &&
          inputStreamSizes[idx] > 0) {
        output.append(data + inputOffsets[idx], inputStreamSizes[idx]);
      }
    }
  }

  // Write trailer.
  detail::writeTrailer(
      options_.projectVersion,
      outputStreamSizes,
      options_.streamSizesEncodingType,
      streamSizesEncodingBuffer_,
      output);
}

std::vector<folly::IOBuf> Projector::project(
    const std::vector<std::string_view>& inputs) const {
  std::vector<folly::IOBuf> results;
  results.reserve(inputs.size());
  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

std::vector<folly::IOBuf> Projector::project(
    const std::vector<folly::IOBuf>& inputs) const {
  std::vector<folly::IOBuf> results;
  results.reserve(inputs.size());
  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

} // namespace facebook::nimble::serde
