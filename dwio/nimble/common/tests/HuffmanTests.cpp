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
#include <gtest/gtest.h>
#include "dwio/nimble/common/Huffman.h"

using namespace ::facebook;

class HuffmanTests : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool();
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(HuffmanTests, EndToEnd) {
  const std::vector<uint32_t> counts = {64, 32, 16, 8, 4, 1, 1, 1, 1};
  // Note that the counts aren't reflective of the data. That's okay. It will
  // still work just fine as long as the data lies in the range
  // [0, counts.size()).
  const std::vector<uint32_t> data = {0, 0, 1, 3, 0, 6, 7, 8, 0, 0, 5};
  int treeDepth = 0;
  auto encodingTable =
      nimble::huffman::generateHuffmanEncodingTable(counts, &treeDepth);
  ASSERT_EQ(treeDepth, 7);
  std::string huffmanEncoded =
      nimble::huffman::huffmanEncode<uint32_t>(encodingTable, data);
  std::vector<uint32_t> recovered(data.size());
  nimble::huffman::DecodingTable decodingTable(
      *pool_, encodingTable, treeDepth);
  decodingTable.decode(data.size(), huffmanEncoded.data(), recovered.data());
  for (int i = 0; i < data.size(); ++i) {
    ASSERT_EQ(recovered[i], data[i]);
  }
}

TEST_F(HuffmanTests, MaxDepthEndToEnd) {
  std::vector<uint32_t> counts = {64, 32, 16, 8, 4, 1, 1, 1, 1};
  const std::vector<int64_t> data = {4, 2, 1, 0, 0, 6, 7, 8, 8};
  int treeDepth;
  auto encodingTable =
      nimble::huffman::generateHuffmanEncodingTableWithMaxDepth(
          counts, 5, &treeDepth);
  ASSERT_LE(treeDepth, 5);
  std::string huffmanEncoded =
      nimble::huffman::huffmanEncode<int64_t>(encodingTable, data);
  std::vector<uint32_t> recovered(data.size());
  nimble::huffman::DecodingTable decodingTable(
      *pool_, encodingTable, treeDepth);
  decodingTable.decode(data.size(), huffmanEncoded.data(), recovered.data());
  for (int i = 0; i < data.size(); ++i) {
    ASSERT_EQ(recovered[i], data[i]);
  }
}

TEST_F(HuffmanTests, StreamedEndToEnd) {
  const std::vector<uint32_t> counts = {7, 8, 9, 10, 11, 12, 13};
  const std::vector<uint32_t> data1 = {0, 1, 2, 3, 4, 5, 6};
  const std::vector<uint32_t> data2 = {6, 6, 5, 5, 4, 0, 1};
  int treeDepth;
  auto encodingTable =
      nimble::huffman::generateHuffmanEncodingTable(counts, &treeDepth);
  ASSERT_LE(treeDepth, counts.size() - 1);
  std::string huffmanEncoded1 =
      nimble::huffman::huffmanEncode<uint32_t>(encodingTable, data1);
  std::string huffmanEncoded2 =
      nimble::huffman::huffmanEncode<uint32_t>(encodingTable, data2);
  std::vector<uint32_t> recovered1(data1.size());
  std::vector<uint32_t> recovered2(data2.size());
  nimble::huffman::DecodingTable decodingTable(
      *pool_, encodingTable, treeDepth);
  decodingTable.decodeStreamed(
      data1.size(),
      huffmanEncoded1.data(),
      huffmanEncoded2.data(),
      recovered1.data(),
      recovered2.data());
  for (int i = 0; i < data1.size(); ++i) {
    ASSERT_EQ(recovered1[i], data1[i]);
  }
  for (int i = 0; i < data2.size(); ++i) {
    ASSERT_EQ(recovered2[i], data2[i]);
  }
}
