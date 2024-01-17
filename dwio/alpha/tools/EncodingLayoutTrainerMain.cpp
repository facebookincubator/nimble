// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <optional>

#include "common/files/FileUtil.h"
#include "common/init/light.h"
#include "common/strings/Zstd.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/tools/EncodingLayoutTrainer.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/experimental/io/FsUtil.h" // @donotremove

using namespace ::facebook;

DEFINE_string(
    input_files,
    "",
    "Input files to process and train cpatured encodings on. "
    "Files are separated by semicolon (;).");

DEFINE_string(
    output_file,
    "",
    "Output captured file. If file exists, it will be overwritten.");

DEFINE_bool(
    compressed,
    true,
    "If true, output file will be compressed using Zstd. Default is true.");

DEFINE_string(
    serialized_serde,
    "",
    "Serialized version of table metadata serde information. "
    "Serialized payload is serialized using Thrift Compact protocol, Zstd compressed and Base64 encoded.");

DEFINE_int32(
    thread_count,
    std::max<uint32_t>(std::thread::hardware_concurrency() - 2, 1),
    "Number of threads to use when training. Default is number of cores on the machine minus 2.");

std::string compress(std::string_view data) {
  if (!FLAGS_compressed) {
    return std::string{data};
  }
  std::string output;
  strings::zstdCompress(data, &output);
  return output;
}

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};
  auto pool = velox::memory::deprecatedAddDefaultLeafMemoryPool();

  if (FLAGS_input_files.empty()) {
    LOG(ERROR) << "Missing input files";
    return 1;
  }

  if (FLAGS_output_file.empty()) {
    LOG(ERROR) << "Missing output file";
    return 1;
  }

  if (folly::fs::exists(FLAGS_output_file)) {
    folly::fs::remove(FLAGS_output_file);
  }

  std::vector<std::string_view> files;
  folly::split(';', FLAGS_input_files, files);

  alpha::tools::EncodingLayoutTrainer trainer{
      *pool, files, FLAGS_serialized_serde};

  folly::CPUThreadPoolExecutor executor{
      static_cast<size_t>(FLAGS_thread_count)};
  auto layoutTree = trainer.train(executor);
  std::string output;
  output.resize(1 * 1024 * 1024);
  auto size = layoutTree.serialize(output);

  LOG(INFO) << "Serialized " << size << " layout bytes";

  files::FileUtil::writeStringToFile(
      compress({output.data(), size}), FLAGS_output_file);

  return 0;
}