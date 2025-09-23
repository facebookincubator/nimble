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
#pragma once

namespace facebook::nimble {

// Attempt to compress a data stream only if the data size is equal or greater
// than this threshold.
// WARNING: These values have been derived experimentally.
constexpr uint32_t kMetaInternalMinCompressionSize = 40;
constexpr uint32_t kZstdMinCompressionSize = 25;

} // namespace facebook::nimble
