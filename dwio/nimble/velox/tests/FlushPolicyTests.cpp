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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "dwio/nimble/velox/FlushPolicy.h"

namespace facebook::nimble {

class ChunkFlushPolicyTest : public ::testing::Test {};

TEST_F(ChunkFlushPolicyTest, InitialNoMemoryPressure) {
  ChunkFlushPolicy policy(
      {.writerMaxMemoryBytes = 1000,
       .writerMinMemoryBytes = 500,
       .targetStripeSizeBytes = 800,
       .compressionRatio = 1.5});

  StripeProgress progress{
      .stripeRawSize = 100,
      .stripeEncodedSize = 50,
      .stripeEncodedRawSize = 80};

  // Total Memory: 100 + 50 = 150 < 1000 (writerMaxMemoryBytes)
  EXPECT_EQ(policy.shouldChunk(progress), FlushDecision::None);

  // Expected Encoded Memory calculation:
  // historicalCompressionRatio = stripeEncodedRawSize / stripeEncodedSize
  //                            = 80 / 50 = 1.6
  // expectedEncodedStripeSize = stripeEncodedSize + stripeRawSize /
  //                             (historicalCompressionRatio *
  //                             max(compressionRatio, 1.0))
  //                           = 50 + 100 / (1.6 * max(1.5, 1.0))
  //                           = 50 + 100 / (1.6 * 1.5)
  //                           = 50 + 100 / 2.4
  //                           = 50 + 41.67 = 91.67 < 800
  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::None);
}

TEST_F(ChunkFlushPolicyTest, MemoryPressureTriggersChunking) {
  ChunkFlushPolicy policy(
      {.writerMaxMemoryBytes = 1000,
       .writerMinMemoryBytes = 500,
       .targetStripeSizeBytes = 800,
       .compressionRatio = 1.5});

  StripeProgress progress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400};
  // Total Memory: 600 + 500 = 1100 > 1000 (writerMaxMemoryBytes)
  EXPECT_EQ(policy.shouldChunk(progress), FlushDecision::Chunk);

  // Mock successful chunk call
  StripeProgress progress2{
      .stripeRawSize = 550,
      .stripeEncodedSize = 520,
      .stripeEncodedRawSize = 400};
  // Total Memory: 550 + 520 = 1070 > 500 (writerMinMemoryBytes)
  EXPECT_EQ(policy.shouldChunk(progress2), FlushDecision::Chunk);

  // Mock memory pressure successfully relieved
  StripeProgress progress3{
      .stripeRawSize = 250,
      .stripeEncodedSize = 200,
      .stripeEncodedRawSize = 400};
  // Total Memory: 250 + 200 = 450 < 500 (writerMinMemoryBytes)
  EXPECT_EQ(policy.shouldChunk(progress3), FlushDecision::None);
}

TEST_F(ChunkFlushPolicyTest, MemoryPressureTriggersFlush) {
  ChunkFlushPolicy policy(
      {.writerMaxMemoryBytes = 1000,
       .writerMinMemoryBytes = 500,
       .targetStripeSizeBytes = 800,
       .compressionRatio = 1.5});

  StripeProgress progress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400};
  // Total Memory: 600 + 500 = 1100 > 1000 (writerMaxMemoryBytes)
  EXPECT_EQ(policy.shouldChunk(progress), FlushDecision::Chunk);

  // Chunking fails to relieve memory pressure
  EXPECT_EQ(policy.shouldChunk(progress), FlushDecision::Stripe);
}

TEST_F(ChunkFlushPolicyTest, StripeSizeTriggersFlush) {
  ChunkFlushPolicy policy(
      {.writerMaxMemoryBytes = 2000,
       .writerMinMemoryBytes = 500,
       .targetStripeSizeBytes = 800,
       .compressionRatio = 1.5});

  StripeProgress progress{
      .stripeRawSize = 1000,
      .stripeEncodedSize = 400,
      .stripeEncodedRawSize = 600};

  // Expected Encoded Memory calculation:
  // historicalCompressionRatio = stripeEncodedRawSize / stripeEncodedSize
  //                            = 600 / 400 = 1.5
  // expectedEncodedStripeSize = stripeEncodedSize + stripeRawSize /
  //                             (historicalCompressionRatio *
  //                             max(compressionRatio, 1.0))
  //                           = 400 + 1000 / (1.5 * max(1.5, 1.0))
  //                           = 400 + 1000 / (1.5 * 1.5)
  //                           = 400 + 1000 / 2.25
  //                           = 400 + 444.44 = 844.44 > 800
  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::Stripe);
}

TEST(FlushPolicyTest, StripeRawSizeFlushPolicyTest) {
  StripeRawSizeFlushPolicy policy(/*stripeRawSize=*/1000);

  StripeProgress progress{
      .stripeRawSize = 1200, // Exceeds threshold
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 800};

  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::Stripe);

  StripeProgress progress2{
      .stripeRawSize = 1000, // At threshold
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 800};

  EXPECT_EQ(policy.shouldFlush(progress2), FlushDecision::Stripe);

  StripeProgress progress3{
      .stripeRawSize = 800, // Below threshold
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 700};

  EXPECT_EQ(policy.shouldFlush(progress3), FlushDecision::None);
}

TEST(FlushPolicyTest, LambdaFlushPolicyTest) {
  auto lambda = [](const StripeProgress& progress) {
    if (progress.stripeRawSize > 1000) {
      return FlushDecision::Stripe;
    } else if (progress.stripeEncodedSize > 500) {
      return FlushDecision::Chunk;
    }
    return FlushDecision::None;
  };

  LambdaFlushPolicy policy(lambda);

  // Test None case
  StripeProgress noneProgress{
      .stripeRawSize = 300,
      .stripeEncodedSize = 200,
      .stripeEncodedRawSize = 250};
  EXPECT_EQ(policy.shouldFlush(noneProgress), FlushDecision::None);

  // Test Chunk case
  StripeProgress chunkProgress{
      .stripeRawSize = 500,
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 450};
  EXPECT_EQ(policy.shouldFlush(chunkProgress), FlushDecision::Chunk);

  // Test Stripe case
  StripeProgress stripeProgress{
      .stripeRawSize = 1200,
      .stripeEncodedSize = 400,
      .stripeEncodedRawSize = 800};
  EXPECT_EQ(policy.shouldFlush(stripeProgress), FlushDecision::Stripe);
}

} // namespace facebook::nimble
