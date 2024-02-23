// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Singleton.h>
#include <folly/init/Init.h>
#include <folly/json/DynamicConverter.h>
#include <folly/json/json.h>
#include "dwio/alpha/tools/AlphaPerfTuningUtils.h"
#include "velox/common/base/StatsReporter.h"

DEFINE_string(input_file, "", "Source file to use for tuning");
DEFINE_string(output_file, "/tmp/rewrite.alpha", "Output path");
DEFINE_string(format, "dwrf", "File format");
DEFINE_string(serde, "", "Serde info");
DEFINE_int32(raw_stripe_size_mb, 0, "Raw stripe size in MB");
DEFINE_int32(write_iters, 10, "Number of write iterations");
DEFINE_int32(read_iters, 10, "Number of read iterations");
DEFINE_int32(read_batch_size, 25, "Number of rows per reader batch");
DEFINE_int32(
    feature_shuffle_seed,
    0,
    "Feature shuffle seed for Feature Projection simulation on read measurements");
DEFINE_bool(
    writer_low_memory_mode,
    false,
    "Writer input buffer to grow accurately per write");

using namespace facebook;
using namespace facebook::alpha::tools;

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  auto ad = dwio::common::request::AccessDescriptorBuilder{}
                .withClientId("alpha_perf_tune")
                .build();
  velox::dwio::common::FileFormat format;
  if (FLAGS_format == "dwrf") {
    format = velox::dwio::common::FileFormat::DWRF;
  } else if (FLAGS_format == "alpha") {
    format = velox::dwio::common::FileFormat::ALPHA;
  } else {
    VELOX_USER_FAIL(
        "Unrecognized format config for alpha perf tuning input file");
  }
  auto perfSummary = PerfTuningUtils::evaluatePerf(
      FLAGS_input_file,
      format,
      ad,
      FLAGS_output_file,
      FLAGS_serde.empty()
          ? std::map<std::string, std::string>{}
          : folly::convertTo<std::map<std::string, std::string>>(
                folly::parseJson(FLAGS_serde)),
      [](auto& options) {
        options.lowMemoryMode = FLAGS_writer_low_memory_mode;
        if (FLAGS_raw_stripe_size_mb > 0) {
          options.flushPolicyFactory = []() {
            return std::make_unique<alpha::RawStripeSizeFlushPolicy>(
                folly::to<uint64_t>(FLAGS_raw_stripe_size_mb) << 20);
          };
        }
      },
      FLAGS_write_iters,
      FLAGS_read_iters,
      FLAGS_feature_shuffle_seed == 0 ? time(nullptr)
                                      : FLAGS_feature_shuffle_seed,
      FLAGS_read_batch_size);

  LOG(INFO) << folly::toPrettyJson(perfSummary.serialize());
}

// Initialize dummy Velox stats reporter
folly::Singleton<velox::BaseStatsReporter> reporter([]() {
  return new velox::DummyStatsReporter();
});
