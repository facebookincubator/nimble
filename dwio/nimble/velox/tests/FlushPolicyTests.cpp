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

class ChunkFlushPolicyTest : public ::testing::Test {
 protected:
  const ChunkFlushPolicyConfig config_{
      .writerMaxMemoryBytes = 1000,
      .writerMinMemoryBytes = 500,
      .targetStripeSizeBytes = 800,
      .compressionRatioFactor = 1.5};
};

TEST_F(ChunkFlushPolicyTest, InitialStateReturnsNone) {
  ChunkFlushPolicy policy(config_);

  StripeProgress progress{
      .stripeRawSize = 100,
      .stripeEncodedSize = 50,
      .stripeEncodedRawSize = 80,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::None);
}

TEST_F(ChunkFlushPolicyTest, TriggersChunkWhenMemoryExceedsMax) {
  ChunkFlushPolicy policy(config_);

  // Total memory: 600 + 500 = 1100 > 1000 (max)
  StripeProgress progress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::Chunk);
}

TEST_F(ChunkFlushPolicyTest, ContinuesChunkingWhenSuccessfulAndAboveMin) {
  ChunkFlushPolicy policy(config_);

  // First call to trigger chunking
  StripeProgress firstProgress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(firstProgress), FlushDecision::Chunk);

  // Second call with successful chunking and memory still above min
  StripeProgress secondProgress{
      .stripeRawSize = 300,
      .stripeEncodedSize = 300,
      .stripeEncodedRawSize = 250,
      .successfullyChunked = true};

  EXPECT_EQ(policy.shouldFlush(secondProgress), FlushDecision::Chunk);
}

TEST_F(ChunkFlushPolicyTest, FlushesStripeWhenChunkingFailsAndAboveMin) {
  ChunkFlushPolicy policy(config_);

  // First call to trigger chunking
  StripeProgress firstProgress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(firstProgress), FlushDecision::Chunk);

  // Second call with failed chunking and memory still above min
  StripeProgress secondProgress{
      .stripeRawSize = 550,
      .stripeEncodedSize = 700,
      .stripeEncodedRawSize = 600,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(secondProgress), FlushDecision::Stripe);
}

TEST_F(ChunkFlushPolicyTest, NoStripeFlushWhenChunkingFailsResetAndAboveMin) {
  ChunkFlushPolicy policy(config_);

  // First call to trigger chunking
  StripeProgress firstProgress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(firstProgress), FlushDecision::Chunk);

  policy.reset();

  // Second call with still chunking instead of flush because reset is called
  StripeProgress secondProgress{
      .stripeRawSize = 550,
      .stripeEncodedSize = 700,
      .stripeEncodedRawSize = 600,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(secondProgress), FlushDecision::Chunk);
}

TEST_F(ChunkFlushPolicyTest, StopsChunkingWhenMemoryBelowMin) {
  ChunkFlushPolicy policy(config_);

  // First call to trigger chunking
  StripeProgress firstProgress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(firstProgress), FlushDecision::Chunk);

  // Second call with memory below min threshold
  StripeProgress secondProgress{
      .stripeRawSize = 200,
      .stripeEncodedSize = 200,
      .stripeEncodedRawSize = 150,
      .successfullyChunked = true};

  // Should not return Chunk, should proceed to storage optimization
  FlushDecision decision = policy.shouldFlush(secondProgress);
  EXPECT_NE(decision, FlushDecision::Chunk);
}

TEST_F(ChunkFlushPolicyTest, OptimizesStorageSizeWhenNoMemoryPressure) {
  ChunkFlushPolicy policy(config_);

  // Memory usage below max, should optimize for storage
  StripeProgress progress{
      .stripeRawSize = 200,
      .stripeEncodedSize = 100,
      .stripeEncodedRawSize = 150,
      .successfullyChunked = false};

  // Expected encoded stripe size = 100 + (1.5 * (150/100)) * 200 = 100 + 450 =
  // 550 550 < 800 (target), so should return None
  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::None);
}

TEST_F(ChunkFlushPolicyTest, FlushesStripeWhenExpectedSizeExceedsTarget) {
  ChunkFlushPolicy policy(config_);

  // Memory usage below max, but expected stripe size exceeds target
  StripeProgress progress{
      .stripeRawSize = 400,
      .stripeEncodedSize = 200,
      .stripeEncodedRawSize = 300,
      .successfullyChunked = false};

  // Expected encoded stripe size = 200 + (1.5 * (300/200)) * 400 = 200 + 900 =
  // 1100 1100 > 800 (target), so should return Stripe
  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::Stripe);
}

TEST_F(ChunkFlushPolicyTest, HandlesZeroEncodedSizeCorrectly) {
  ChunkFlushPolicy policy(config_);

  // Case 1: High memory pressure with zero encoded size
  StripeProgress highMemoryProgress{
      .stripeRawSize = 1500, // Large raw size creates memory pressure
      .stripeEncodedSize = 0, // But no encoded data yet
      .stripeEncodedRawSize = 0,
      .successfullyChunked = false};

  // Memory pressure: 1500 + 0 = 1500 > 1000 (max), so should trigger chunking
  // Even though encoded size is 0, memory pressure takes priority
  EXPECT_EQ(policy.shouldFlush(highMemoryProgress), FlushDecision::Chunk);

  // Case 2: Low memory usage with zero encoded size
  ChunkFlushPolicy policy2(config_); // Fresh policy instance
  StripeProgress lowMemoryProgress{
      .stripeRawSize = 200, // Low raw size, no memory pressure
      .stripeEncodedSize = 0, // No encoded data yet
      .stripeEncodedRawSize = 0,
      .successfullyChunked = false};

  // Memory usage: 200 + 0 = 200 < 1000 (max), no memory pressure
  // Early return due to stripeEncodedSize == 0 should return None
  EXPECT_EQ(policy2.shouldFlush(lowMemoryProgress), FlushDecision::None);
}

TEST_F(ChunkFlushPolicyTest, HandlesZeroCompressionRatioFactor) {
  ChunkFlushPolicyConfig zeroCompressionConfig{
      .writerMaxMemoryBytes = 1000,
      .writerMinMemoryBytes = 500,
      .targetStripeSizeBytes = 300,
      .compressionRatioFactor = 0.0};

  ChunkFlushPolicy policy(zeroCompressionConfig);

  StripeProgress progress{
      .stripeRawSize = 400,
      .stripeEncodedSize = 200,
      .stripeEncodedRawSize = 300,
      .successfullyChunked = false};

  // Expected encoded stripe size = 200 + (0.0 * (300/200)) * 400 = 200 + 0 =
  // 200 200 < 300 (target), so should return None
  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::None);
}

TEST_F(
    ChunkFlushPolicyTest,
    MemoryPressureTakesPriorityOverStorageOptimization) {
  ChunkFlushPolicy policy(config_);

  // High memory usage that would trigger chunking
  StripeProgress progress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 50, // Very small encoded raw size
      .successfullyChunked = false};

  // Even though compression ratio calculation might suggest stripe
  // optimization, memory pressure should take priority
  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::Chunk);
}

// Test the sequence of decisions through a complete cycle
TEST_F(ChunkFlushPolicyTest, CompleteChunkingCycle) {
  ChunkFlushPolicy policy(config_);

  // 1. Initial state - no memory pressure
  StripeProgress initialProgress{
      .stripeRawSize = 100,
      .stripeEncodedSize = 50,
      .stripeEncodedRawSize = 80,
      .successfullyChunked = false};
  EXPECT_EQ(policy.shouldFlush(initialProgress), FlushDecision::None);

  // 2. Memory grows, triggers chunking
  StripeProgress highMemoryProgress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 500,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};
  EXPECT_EQ(policy.shouldFlush(highMemoryProgress), FlushDecision::Chunk);

  // 3. Chunking succeeds, continue chunking while above min
  StripeProgress successfulChunkProgress{
      .stripeRawSize = 300,
      .stripeEncodedSize = 300,
      .stripeEncodedRawSize = 250,
      .successfullyChunked = true};
  EXPECT_EQ(policy.shouldFlush(successfulChunkProgress), FlushDecision::Chunk);

  // 4. Memory drops below min, switches to storage optimization
  StripeProgress lowMemoryProgress{
      .stripeRawSize = 200,
      .stripeEncodedSize = 200,
      .stripeEncodedRawSize = 150,
      .successfullyChunked = true};
  // Should not chunk anymore, should optimize for storage
  // Expected encoded stripe size = 200 + (1.5 * (150/200)) * 200 = 200 + 225 =
  // 425 425 < 800 (target), so should return None
  EXPECT_EQ(policy.shouldFlush(lowMemoryProgress), FlushDecision::None);
}

class RawStripeSizeFlushPolicyTest : public ::testing::Test {
 protected:
  static constexpr uint64_t RAW_STRIPE_SIZE = 1000;
};

TEST_F(RawStripeSizeFlushPolicyTest, FlushesWhenRawSizeExceedsThreshold) {
  RawStripeSizeFlushPolicy policy(RAW_STRIPE_SIZE);

  StripeProgress progress{
      .stripeRawSize = 1200, // Exceeds threshold
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 800,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::Stripe);
}

TEST_F(RawStripeSizeFlushPolicyTest, DoesNotFlushWhenBelowThreshold) {
  RawStripeSizeFlushPolicy policy(RAW_STRIPE_SIZE);

  StripeProgress progress{
      .stripeRawSize = 800, // Below threshold
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 700,
      .successfullyChunked = false};

  EXPECT_EQ(policy.shouldFlush(progress), FlushDecision::None);
}

class LambdaFlushPolicyTest : public ::testing::Test {};

TEST_F(LambdaFlushPolicyTest, CallsProvidedLambda) {
  bool lambdaCalled = false;
  auto lambda = [&lambdaCalled](const StripeProgress& progress) {
    lambdaCalled = true;
    return progress.stripeRawSize > 500 ? FlushDecision::Stripe
                                        : FlushDecision::None;
  };

  LambdaFlushPolicy policy(lambda);

  StripeProgress progress{
      .stripeRawSize = 600,
      .stripeEncodedSize = 300,
      .stripeEncodedRawSize = 400,
      .successfullyChunked = false};

  FlushDecision decision = policy.shouldFlush(progress);

  EXPECT_TRUE(lambdaCalled);
  EXPECT_EQ(decision, FlushDecision::Stripe);
}

TEST_F(LambdaFlushPolicyTest, ReturnsCorrectDecisionBasedOnLambda) {
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
      .stripeEncodedRawSize = 250,
      .successfullyChunked = false};
  EXPECT_EQ(policy.shouldFlush(noneProgress), FlushDecision::None);

  // Test Chunk case
  StripeProgress chunkProgress{
      .stripeRawSize = 500,
      .stripeEncodedSize = 600,
      .stripeEncodedRawSize = 450,
      .successfullyChunked = false};
  EXPECT_EQ(policy.shouldFlush(chunkProgress), FlushDecision::Chunk);

  // Test Stripe case
  StripeProgress stripeProgress{
      .stripeRawSize = 1200,
      .stripeEncodedSize = 400,
      .stripeEncodedRawSize = 800,
      .successfullyChunked = false};
  EXPECT_EQ(policy.shouldFlush(stripeProgress), FlushDecision::Stripe);
}

} // namespace facebook::nimble
