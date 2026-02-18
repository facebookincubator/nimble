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
      ChunkFlushPolicyConfig{
          .writerMemoryHighThresholdBytes = 1000,
          .writerMemoryLowThresholdBytes = 500,
          .targetStripeSizeBytes = 800,
          .estimatedCompressionFactor = 1.5});

  StripeProgress progress{
      .stripeRawSize = 100,
      .stripeEncodedSize = 50,
      .stripeEncodedLogicalSize = 80};

  // Total Memory: 100 + 50 = 150 < 1000 (writerMemoryHighThresholdBytes)
  EXPECT_FALSE(policy.shouldChunk(progress));

  // shouldFlush checks two conditions:
  // 1. Memory pressure check: 100 + 50 = 150 < 1000 (passes, no flush)
  // 2. Stripe size check (only if condition 1 passes):
  //    compressionFactor = stripeEncodedLogicalSize / stripeEncodedSize
  //                      = 80 / 50 = 1.6
  //    expectedEncodedStripeSize = stripeEncodedSize + stripeRawSize /
  //                                max(compressionFactor, 1.0)
  //                              = 50 + 100 / max(1.6, 1.0)
  //                              = 50 + 100 / 1.6
  //                              = 50 + 62.5 = 112.5 < 800
  //                              (targetStripeSizeBytes)
  EXPECT_FALSE(policy.shouldFlush(progress));
  ;
}

TEST_F(ChunkFlushPolicyTest, MemoryPressureChunkingLifecycle) {
  ChunkFlushPolicy policy(
      ChunkFlushPolicyConfig{
          .writerMemoryHighThresholdBytes = 1000,
          .writerMemoryLowThresholdBytes = 500,
          .targetStripeSizeBytes = 800,
          .estimatedCompressionFactor = 1.5});

  // Start chunking when memory exceeds high threshold
  StripeProgress progress1{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedLogicalSize = 400};
  // Total Memory: 600 + 500 = 1100 > 1000 (writerMemoryHighThresholdBytes)
  EXPECT_TRUE(policy.shouldChunk(progress1));

  // Continue chunking with decreasing raw size but still above low threshold
  // Raw size decreased (600 → 550) and memory still above low threshold
  StripeProgress progress2{
      .stripeRawSize = 550,
      .stripeEncodedSize = 520,
      .stripeEncodedLogicalSize = 400};
  // Total Memory: 550 + 520 = 1070 > 500 (writerMemoryLowThresholdBytes)
  EXPECT_TRUE(policy.shouldChunk(progress2));

  // Continue chunking as raw size decreases further
  StripeProgress progress3{
      .stripeRawSize = 300,
      .stripeEncodedSize = 300,
      .stripeEncodedLogicalSize = 400};
  // Total memory: 300 + 300 = 600 > 500
  EXPECT_TRUE(policy.shouldChunk(progress3));

  // Memory drops below low threshold - stop chunking
  StripeProgress progress4{
      .stripeRawSize = 200,
      .stripeEncodedSize = 200,
      .stripeEncodedLogicalSize = 400};
  // Total Memory: 200 + 200 = 400 < 500 (writerMemoryLowThresholdBytes)
  EXPECT_FALSE(policy.shouldChunk(progress4));
}

TEST_F(ChunkFlushPolicyTest, MemoryPressureTriggersFlush) {
  ChunkFlushPolicy policy(
      ChunkFlushPolicyConfig{
          .writerMemoryHighThresholdBytes = 1000,
          .writerMemoryLowThresholdBytes = 500,
          .targetStripeSizeBytes = 800,
          .estimatedCompressionFactor = 1.5});

  StripeProgress progress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedLogicalSize = 400};
  // Total Memory: 600 + 500 = 1100 > 1000 (writerMemoryHighThresholdBytes)
  EXPECT_TRUE(policy.shouldChunk(progress));

  // Total Memory: 600 + 500 = 1100 > 1000 (writerMemoryHighThresholdBytes)
  EXPECT_TRUE(policy.shouldFlush(progress));
}

TEST_F(ChunkFlushPolicyTest, StripeSizeTriggersFlush) {
  ChunkFlushPolicy policy(
      ChunkFlushPolicyConfig{
          .writerMemoryHighThresholdBytes = 2000,
          .writerMemoryLowThresholdBytes = 500,
          .targetStripeSizeBytes = 800,
          .estimatedCompressionFactor = 1.5});

  StripeProgress progress{
      .stripeRawSize = 1000,
      .stripeEncodedSize = 400,
      .stripeEncodedLogicalSize = 600};

  // Expected Encoded Memory calculation:
  // compressionFactor = stripeEncodedLogicalSize / stripeEncodedSize
  //                   = 600 / 400 = 1.5
  // expectedEncodedStripeSize = stripeEncodedSize + stripeRawSize /
  //                             max(compressionFactor, 1.0)
  //                           = 400 + 1000 / max(1.5, 1.0)
  //                           = 400 + 1000 / 1.5
  //                           = 400 + 666.67 = 1066.67 > 800
  EXPECT_TRUE(policy.shouldFlush(progress));
  ;
}

TEST_F(ChunkFlushPolicyTest, ZeroEncodedSize) {
  ChunkFlushPolicy policy(
      ChunkFlushPolicyConfig{
          .writerMemoryHighThresholdBytes = 1000,
          .writerMemoryLowThresholdBytes = 500,
          .targetStripeSizeBytes = 800,
          .estimatedCompressionFactor = 2.0});

  StripeProgress progress{
      .stripeRawSize = 1200,
      .stripeEncodedSize = 0,
      .stripeEncodedLogicalSize = 0};

  // Memory pressure check:
  // tripeRawSize + stripeEncodedSize = 1200 + 0 = 1200 > 1000
  // This triggers chunking due to memory pressure
  EXPECT_TRUE(policy.shouldChunk(progress));
  ;

  // Flush check: Memory pressure (1200 > 1000) triggers stripe flush
  EXPECT_TRUE(policy.shouldFlush(progress));
  ;
}

TEST(FlushPolicyTest, StripeRawSizeFlushPolicyTest) {
  StripeRawSizeFlushPolicy policy(/*stripeRawSize=*/1000);

  StripeProgress progress{
      .stripeRawSize = 1200, // Exceeds threshold
      .stripeEncodedSize = 600,
      .stripeEncodedLogicalSize = 800};

  EXPECT_TRUE(policy.shouldFlush(progress));
  ;

  StripeProgress progress2{
      .stripeRawSize = 800, // Below threshold
      .stripeEncodedSize = 600,
      .stripeEncodedLogicalSize = 700};

  EXPECT_FALSE(policy.shouldFlush(progress2));
}

TEST(FlushPolicyTest, LambdaFlushPolicyTest) {
  LambdaFlushPolicy policy(
      /*flushLambda_=*/
      [](const StripeProgress& progress) {
        return progress.stripeRawSize > 1000;
      },
      /*chunkLambda_=*/
      [](const StripeProgress& progress) {
        return progress.stripeEncodedSize > 500;
      });

  // Test None case
  StripeProgress noneProgress{
      .stripeRawSize = 300,
      .stripeEncodedSize = 200,
      .stripeEncodedLogicalSize = 250};
  EXPECT_FALSE(policy.shouldFlush(noneProgress));
  EXPECT_FALSE(policy.shouldChunk(noneProgress));

  // Test Chunk case
  StripeProgress chunkProgress{
      .stripeRawSize = 500,
      .stripeEncodedSize = 600,
      .stripeEncodedLogicalSize = 450};
  EXPECT_TRUE(policy.shouldChunk(chunkProgress));

  // Test Stripe case
  StripeProgress stripeProgress{
      .stripeRawSize = 1200,
      .stripeEncodedSize = 400,
      .stripeEncodedLogicalSize = 800};
  EXPECT_TRUE(policy.shouldFlush(stripeProgress));
}

TEST(FlushPolicyTest, LambdaFlushPolicyDefaultLambdas) {
  LambdaFlushPolicy policy;

  StripeProgress progress{
      .stripeRawSize = 1200,
      .stripeEncodedSize = 600,
      .stripeEncodedLogicalSize = 800};

  EXPECT_FALSE(policy.shouldFlush(progress));
  ;
  EXPECT_FALSE(policy.shouldChunk(progress));
}

} // namespace facebook::nimble
