// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "dwio/nimble/common/Checksum.h"
#include "dwio/nimble/common/Exceptions.h"

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace facebook::nimble {

namespace {
class Xxh3_64Checksum : public Checksum {
 public:
  Xxh3_64Checksum() : state_{XXH3_createState()} {
    NIMBLE_DASSERT(state_ != nullptr, "Failed to initialize Xxh3_64Checksum.");
    reset();
  }

  ~Xxh3_64Checksum() override {
    XXH3_freeState(state_);
  }

  void update(std::string_view data) override {
    auto result = XXH3_64bits_update(state_, data.data(), data.size());
    NIMBLE_ASSERT(result != XXH_ERROR, "XXH3_64bits_update error.");
  }

  uint64_t getChecksum(bool reset) override {
    auto ret = static_cast<uint64_t>(XXH3_64bits_digest(state_));
    if (UNLIKELY(reset)) {
      this->reset();
    }
    return ret;
  }

  ChecksumType getType() const override {
    return ChecksumType::XXH3_64;
  }

 private:
  XXH3_state_t* state_;

  void reset() {
    auto result = XXH3_64bits_reset(state_);
    NIMBLE_ASSERT(result != XXH_ERROR, "XXH3_64bits_reset error.");
  }
};
} // namespace

std::unique_ptr<Checksum> ChecksumFactory::create(ChecksumType type) {
  switch (type) {
    case ChecksumType::XXH3_64:
      return std::make_unique<Xxh3_64Checksum>();
    default:
      NIMBLE_NOT_SUPPORTED(
          fmt::format("Unsupported checksum type: {}", toString(type)));
  }
}

} // namespace facebook::nimble
