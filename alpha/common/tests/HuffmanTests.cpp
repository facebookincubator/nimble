// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>
#include "dwio/alpha/common/Huffman.h"

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
      alpha::huffman::generateHuffmanEncodingTable(counts, &treeDepth);
  ASSERT_EQ(treeDepth, 7);
  std::string huffmanEncoded =
      alpha::huffman::huffmanEncode<uint32_t>(encodingTable, data);
  std::vector<uint32_t> recovered(data.size());
  alpha::huffman::DecodingTable decodingTable(*pool_, encodingTable, treeDepth);
  decodingTable.decode(data.size(), huffmanEncoded.data(), recovered.data());
  for (int i = 0; i < data.size(); ++i) {
    ASSERT_EQ(recovered[i], data[i]);
  }
}

TEST_F(HuffmanTests, MaxDepthEndToEnd) {
  std::vector<uint32_t> counts = {64, 32, 16, 8, 4, 1, 1, 1, 1};
  const std::vector<int64_t> data = {4, 2, 1, 0, 0, 6, 7, 8, 8};
  int treeDepth;
  auto encodingTable = alpha::huffman::generateHuffmanEncodingTableWithMaxDepth(
      counts, 5, &treeDepth);
  ASSERT_LE(treeDepth, 5);
  std::string huffmanEncoded =
      alpha::huffman::huffmanEncode<int64_t>(encodingTable, data);
  std::vector<uint32_t> recovered(data.size());
  alpha::huffman::DecodingTable decodingTable(*pool_, encodingTable, treeDepth);
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
      alpha::huffman::generateHuffmanEncodingTable(counts, &treeDepth);
  ASSERT_LE(treeDepth, counts.size() - 1);
  std::string huffmanEncoded1 =
      alpha::huffman::huffmanEncode<uint32_t>(encodingTable, data1);
  std::string huffmanEncoded2 =
      alpha::huffman::huffmanEncode<uint32_t>(encodingTable, data2);
  std::vector<uint32_t> recovered1(data1.size());
  std::vector<uint32_t> recovered2(data2.size());
  alpha::huffman::DecodingTable decodingTable(*pool_, encodingTable, treeDepth);
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
