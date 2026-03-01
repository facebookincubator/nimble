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

namespace facebook::nimble {

std::string toString(SerializationVersion version) {
  switch (version) {
    case SerializationVersion::kDense:
      return "kDense";
    case SerializationVersion::kSparse:
      return "kSparse";
    case SerializationVersion::kDenseEncoded:
      return "kDenseEncoded";
    case SerializationVersion::kSparseEncoded:
      return "kSparseEncoded";
  }
  return fmt::format("Unknown({})", static_cast<uint8_t>(version));
}

bool SerializerOptions::hasVersionHeader() const {
  return version.has_value();
}

SerializationVersion SerializerOptions::serializationVersion() const {
  return version.value_or(SerializationVersion::kDense);
}

bool SerializerOptions::enableEncoding() const {
  auto v = serializationVersion();
  return v == SerializationVersion::kDenseEncoded ||
      v == SerializationVersion::kSparseEncoded;
}

bool SerializerOptions::denseFormat() const {
  return !sparseFormat();
}

bool SerializerOptions::sparseFormat() const {
  auto v = serializationVersion();
  return v == SerializationVersion::kSparse ||
      v == SerializationVersion::kSparseEncoded;
}

bool DeserializerOptions::hasVersionHeader() const {
  return version.has_value();
}

SerializationVersion DeserializerOptions::serializationVersion() const {
  return version.value_or(SerializationVersion::kDense);
}

bool DeserializerOptions::enableEncoding() const {
  auto v = serializationVersion();
  return v == SerializationVersion::kDenseEncoded ||
      v == SerializationVersion::kSparseEncoded;
}

bool DeserializerOptions::denseFormat() const {
  return !sparseFormat();
}

bool DeserializerOptions::sparseFormat() const {
  const auto v = serializationVersion();
  return v == SerializationVersion::kSparse ||
      v == SerializationVersion::kSparseEncoded;
}

} // namespace facebook::nimble
