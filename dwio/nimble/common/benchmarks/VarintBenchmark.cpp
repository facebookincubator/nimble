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
#include <algorithm>
#include <memory>
#include <vector>

#include "dwio/nimble/common/Varint.h"
#include "folly/Benchmark.h"
#include "folly/Random.h"
#include "folly/Varint.h"

using namespace ::facebook;

const int kNumElements = 1000 * 1000;

// Basically same code as dwrf::IntDecoder::readVuLong.
uint64_t DwrfRead(const char** bufferStart, const char* bufferEnd) {
  if (LIKELY(bufferEnd - *bufferStart >= folly::kMaxVarintLength64)) {
    const char* p = *bufferStart;
    uint64_t val;
    do {
      int64_t b;
      b = *p++;
      val = (b & 0x7f);
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 7;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 14;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 21;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 28;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 35;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 42;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 49;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 56;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x01) << 63;
      if (LIKELY(b >= 0)) {
        break;
      } else {
        throw std::runtime_error{"invalid encoding: likely corrupt data"};
      }
    } while (false);
    *bufferStart = p;
    return val;
  } else {
    // this part isn't the same, but doesn't measurably effect time.
    return nimble::varint::readVarint64(bufferStart);
  }
}

// Makes random data uniform over in bit width over 32 bits.
std::vector<uint32_t> MakeUniformData(int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  for (int i = 0; i < num_elements; ++i) {
    const int bit_shift = 1 + folly::Random::secureRand32() % 32;
    data[i] = folly::Random::secureRand32() % (1 << bit_shift);
  }
  return data;
}

// Makes 95% 1 byte, 5% 2 byte data.
std::vector<uint32_t> MakeSkewedData(int num_elements = kNumElements) {
  std::vector<uint32_t> data(num_elements);
  for (int i = 0; i < num_elements; ++i) {
    if (folly::Random::secureRand32() % 20) {
      data[i] = folly::Random::secureRand32() % (1 << 7);
    } else {
      data[i] = folly::Random::secureRand32() % (1 << 14);
    }
  }
  return data;
}

BENCHMARK(Encode, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  BENCHMARK_SUSPEND {
    data = MakeUniformData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
  }
  while (iters--) {
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
    CHECK_GE(pos - buf.get(), kNumElements);
  }
}

BENCHMARK(FollyEncode, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<uint8_t[]> buf;
  BENCHMARK_SUSPEND {
    data = MakeUniformData();
    buf = std::make_unique<uint8_t[]>(kNumElements * folly::kMaxVarintLength32);
  }
  while (iters--) {
    uint8_t* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      pos += folly::encodeVarint(data[i], pos);
    }
    CHECK_GE(pos - buf.get(), kNumElements);
  }
}

BENCHMARK(NimbleDecodeUniform, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeUniformData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
  }
  while (iters--) {
    const char* cpos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      recovered[i] = nimble::varint::readVarint32(&cpos);
    }
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(NimbleBulkDecodeUniform, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeUniformData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
  }
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(FollyDecodeUniform, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<uint8_t[]> buf;
  uint8_t* pos;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeUniformData();
    buf = std::make_unique<uint8_t[]>(kNumElements * folly::kMaxVarintLength32);
    pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      pos += folly::encodeVarint(data[i], pos);
    }
  }
  while (iters--) {
    const uint8_t* fstart = buf.get();
    const uint8_t* fend = buf.get() + (pos - buf.get());
    folly::Range<const uint8_t*> frange(fstart, fend);
    for (int i = 0; i < kNumElements; ++i) {
      recovered[i] = folly::decodeVarint(frange);
    }
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(DwrfDecodeUniform, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  uint64_t varint_bytes;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeUniformData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
    varint_bytes = pos - buf.get();
  }
  while (iters--) {
    const char* cpos = buf.get();
    const char* end = cpos + varint_bytes;
    for (int i = 0; i < kNumElements; ++i) {
      recovered[i] = DwrfRead(&cpos, end);
    }
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(NimbleDecodeSkewed, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeSkewedData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
  }
  while (iters--) {
    const char* cpos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      recovered[i] = nimble::varint::readVarint32(&cpos);
    }
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(NimbleBulkDecodeSkewed, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeSkewedData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
  }
  while (iters--) {
    const char* cpos = buf.get();
    nimble::varint::bulkVarintDecode32(kNumElements, cpos, recovered.data());
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(FollyDecodeSkewed, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<uint8_t[]> buf;
  uint8_t* pos;
  std::vector<uint32_t> recovered;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeSkewedData();
    buf = std::make_unique<uint8_t[]>(kNumElements * folly::kMaxVarintLength32);
    pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      pos += folly::encodeVarint(data[i], pos);
    }
  }
  while (iters--) {
    const uint8_t* fstart = buf.get();
    const uint8_t* fend = buf.get() + (pos - buf.get());
    folly::Range<const uint8_t*> frange(fstart, fend);
    for (int i = 0; i < kNumElements; ++i) {
      recovered[i] = folly::decodeVarint(frange);
    }
    CHECK_EQ(recovered.back(), data.back());
  }
}

BENCHMARK(DwrfDecodeSkewed, iters) {
  std::vector<uint32_t> data;
  std::unique_ptr<char[]> buf;
  std::vector<uint32_t> recovered;
  uint64_t varint_bytes;
  BENCHMARK_SUSPEND {
    recovered.resize(kNumElements);
    data = MakeSkewedData();
    buf = std::make_unique<char[]>(kNumElements * folly::kMaxVarintLength32);
    char* pos = buf.get();
    for (int i = 0; i < kNumElements; ++i) {
      nimble::varint::writeVarint(data[i], &pos);
    }
    varint_bytes = pos - buf.get();
  }
  while (iters--) {
    const char* cpos = buf.get();
    const char* end = cpos + varint_bytes;
    for (int i = 0; i < kNumElements; ++i) {
      recovered[i] = DwrfRead(&cpos, end);
    }
    CHECK_EQ(recovered.back(), data.back());
  }
}

int main() {
  folly::runBenchmarks();
}
