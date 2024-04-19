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
#include <queue>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Huffman.h"

namespace facebook::nimble::huffman {

std::vector<uint32_t> generateHuffmanEncodingTable(
    std::span<const uint32_t> counts,
    int* treeDepth) {
  struct Element {
    int symbol;
    uint32_t count;
    Element* leftChild;
    Element* rightChild;
    uint32_t encoding;
    uint32_t encodingLength;
  };
  // TODO: Ugh this is actually terrible with unique ptrs. Just have an owning
  // vector of 2x length counts Elements and use raw pointers. Duh.
  std::vector<std::unique_ptr<Element>> pendingElements(counts.size());
  for (int i = 0; i < counts.size(); ++i) {
    pendingElements[i] = std::make_unique<Element>();
    Element* e = pendingElements[i].get();
    e->symbol = i;
    e->count = counts[i];
    e->leftChild = nullptr;
    e->rightChild = nullptr;
  }
  auto compare = [](const std::unique_ptr<Element>& a,
                    const std::unique_ptr<Element>& b) {
    return a->count > b->count;
  };
  std::make_heap(pendingElements.begin(), pendingElements.end(), compare);

  // Get 2 smallest elements, merge them into a new one, repeat.
  std::vector<std::unique_ptr<Element>> doneElements;
  while (pendingElements.size() >= 2) {
    std::pop_heap(pendingElements.begin(), pendingElements.end(), compare);
    std::unique_ptr<Element> leftChild = std::move(pendingElements.back());
    pendingElements.pop_back();
    std::pop_heap(pendingElements.begin(), pendingElements.end(), compare);
    std::unique_ptr<Element> rightChild = std::move(pendingElements.back());
    pendingElements.pop_back();
    std::unique_ptr<Element> merged = std::make_unique<Element>();
    merged->symbol = -1;
    merged->count = leftChild->count + rightChild->count;
    merged->leftChild = leftChild.get();
    merged->rightChild = rightChild.get();
    doneElements.push_back(std::move(leftChild));
    doneElements.push_back(std::move(rightChild));
    pendingElements.push_back(std::move(merged));
    std::push_heap(pendingElements.begin(), pendingElements.end(), compare);
  }

  // Now traverse the tree and fill out the encodings.
  std::vector<uint32_t> entries(counts.size());
  std::queue<Element*> traverse;
  traverse.push(pendingElements.front().get());
  traverse.front()->encoding = 0;
  traverse.front()->encodingLength = 0;
  *treeDepth = 0;
  while (!traverse.empty()) {
    Element* next = traverse.front();
    *treeDepth = std::max(*treeDepth, (int)next->encodingLength);
    traverse.pop();
    if (next->leftChild) {
      next->leftChild->encoding =
          next->encoding | (1UL << next->encodingLength);
      next->leftChild->encodingLength = next->encodingLength + 1;
      traverse.push(next->leftChild);
    }
    if (next->rightChild) {
      next->rightChild->encoding = next->encoding;
      next->rightChild->encodingLength = next->encodingLength + 1;
      traverse.push(next->rightChild);
    }
    // For leaf elements go ahead and output the entries. Remember that
    // depth (encoding length) is lowest 5 bits.
    if (next->symbol != -1) {
      entries[next->symbol] = (next->encoding << 5) | next->encodingLength;
    }
  }
  return entries;
}

namespace {

int minTreeDepth(uint32_t size) {
  int minDepth = 1;
  uint64_t capacity = 2;
  while (capacity < size) {
    ++minDepth;
    capacity <<= 1;
  }
  return minDepth;
}

} // namespace

std::vector<uint32_t> generateHuffmanEncodingTableWithMaxDepth(
    std::span<const uint32_t> counts,
    int maxDepth,
    int* treeDepth) {
  CHECK_GE(maxDepth, minTreeDepth(counts.size()));
  auto table = generateHuffmanEncodingTable(counts, treeDepth);
  if (*treeDepth <= maxDepth) {
    return table;
  }
  std::vector<uint32_t> ownedCounts;
  ownedCounts.reserve(counts.size());
  for (int i = 0; i < counts.size(); ++i) {
    if (counts[i] > 1) {
      ownedCounts.push_back(counts[i] >> 1);
    } else {
      ownedCounts.push_back(1);
    }
  }
  while (true) {
    table = generateHuffmanEncodingTable(ownedCounts, treeDepth);
    if (*treeDepth <= maxDepth) {
      return table;
    }
    // TODO: Might want to shift dynamically based on how far we are from the
    // max depth.
    for (auto& count : ownedCounts) {
      if (count > 1) {
        count >>= 1;
      }
    }
  }
}

DecodingTable::DecodingTable(
    facebook::velox::memory::MemoryPool& memoryPool,
    std::span<const uint32_t> encodingTable,
    int treeDepth)
    : treeDepth_(treeDepth), lookupTable_(&memoryPool, 1 << treeDepth_) {
  CHECK_LE(treeDepth_, 15);
  CHECK_LE(encodingTable.size(), 4096);
  for (uint32_t i = 0; i < encodingTable.size(); ++i) {
    const uint32_t entry = encodingTable[i];
    const uint16_t entryDepth = entry & 15;
    const uint16_t slotBits = treeDepth - entryDepth;
    const uint16_t entrySlots = 1 << slotBits;
    // Remember entry still uses 5 bits for depth even though we only use 4
    // in the in memory table.
    const uint16_t encodingSuffix = entry >> 5;
    const uint16_t lookupTableEntry = (i << 4) | entryDepth;
    for (uint16_t j = 0; j < entrySlots; ++j) {
      lookupTable_.data()[(j << entryDepth) | encodingSuffix] =
          lookupTableEntry;
    }
  }
}

} // namespace facebook::nimble::huffman
