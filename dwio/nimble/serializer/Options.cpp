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

#include "dwio/nimble/serializer/Options.h"

#include "dwio/nimble/common/Exceptions.h"

namespace facebook::nimble {

std::string toString(SerializationVersion version) {
  switch (version) {
    case SerializationVersion::kLegacy:
      return "kLegacy";
    case SerializationVersion::kCompactRaw:
      return "kCompactRaw";
    case SerializationVersion::kTablet:
      return "kTablet";
    default:
      NIMBLE_FAIL(
          "Unknown SerializationVersion: {}", static_cast<int>(version));
  }
}

EncodingType getTrailerEncodingType(EncodingType encodingType) {
  switch (encodingType) {
    case EncodingType::Trivial:
    case EncodingType::Varint:
    case EncodingType::Delta:
    case EncodingType::FixedBitWidth:
    case EncodingType::MainlyConstant:
      return encodingType;
    default:
      NIMBLE_FAIL(
          "Unsupported EncodingType for stream sizes trailer: {}",
          encodingType);
  }
}

EncodingSelectionPolicyFactory defaultEncodingSelectionPolicyFactory() {
  // Initialize the default factory once. It is const and createPolicy() does
  // not mutate it, so a single instance can be safely shared across all default
  // SerializerOptions instead of rebuilding the read-factor vector on every
  // construction. The returned lambda is captureless and refers to the shared
  // factory directly.
  static const ManualEncodingSelectionPolicyFactory factory(
      ManualEncodingSelectionPolicyFactory::defaultReadFactors(),
      /*compressionOptions=*/std::nullopt);
  return [](DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return factory.createPolicy(dataType);
  };
}

bool SerializerOptions::hasVersionHeader() const {
  return version.has_value();
}

SerializationVersion SerializerOptions::serializationVersion() const {
  return version.value_or(SerializationVersion::kLegacy);
}

bool SerializerOptions::enableEncoding() const {
  return isCompactFormat(serializationVersion());
}

} // namespace facebook::nimble
