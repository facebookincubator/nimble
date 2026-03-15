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
    case SerializationVersion::kCompact:
      return "kCompact";
    case SerializationVersion::kCompactRaw:
      return "kCompactRaw";
    default:
      NIMBLE_FAIL(
          "Unknown SerializationVersion: {}", static_cast<int>(version));
  }
}

EncodingType getRawEncodingType(EncodingType encodingType) {
  switch (encodingType) {
    case EncodingType::Trivial:
    case EncodingType::Varint:
      return encodingType;
    default:
      NIMBLE_FAIL("Unsupported EncodingType for kCompactRaw: {}", encodingType);
  }
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

bool DeserializerOptions::hasVersionHeader() const {
  return version.has_value();
}

SerializationVersion DeserializerOptions::serializationVersion() const {
  return version.value_or(SerializationVersion::kLegacy);
}

bool DeserializerOptions::enableEncoding() const {
  return isCompactFormat(serializationVersion());
}

} // namespace facebook::nimble
