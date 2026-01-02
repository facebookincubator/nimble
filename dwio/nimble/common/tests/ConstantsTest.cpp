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

#include <gtest/gtest.h>

#include "dwio/nimble/common/Constants.h"

namespace facebook::nimble::test {

TEST(ConstantsTest, kChunkHeaderSize) {
  // Chunk header format:
  //   - 4 bytes (uint32_t) for compressed chunk length
  //   - 1 byte for compression type
  // This constant must remain 5 to maintain backward compatibility with
  // existing Nimble files.
  EXPECT_EQ(kChunkHeaderSize, 5);
  EXPECT_EQ(kChunkHeaderSize, sizeof(uint32_t) + 1);
}

} // namespace facebook::nimble::test
