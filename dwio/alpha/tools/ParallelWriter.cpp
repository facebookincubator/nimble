// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "common/init/light.h"
#include "dwio/alpha/velox/VeloxWriter.h"
#include "dwio/alpha/velox/VeloxWriterOptions.h"
#include "dwio/common/filesystem/FileSystem.h"
#include "dwio/utils/BufferedWriteFile.h"
#include "dwio/utils/FileSink.h"
#include "dwio/utils/InputStreamFactory.h"
#include "fb_velox/alpha/reader/AlphaReader.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/writer/Writer.h"

DEFINE_string(input_file, "", "Input file to read");
DEFINE_string(
    output_file,
    "",
    "Output file to write. If not specified, writes to memory");
DEFINE_int32(
    threads,
    -1,
    "Defines the # of threads to use in parallel writes. Default is std::hardware_concurrency()");
DEFINE_int32(
    buffered_write_size,
    72 * 1024 * 1024,
    "Set the buffered write size to use when writing to memory. Default is 72MB");
DEFINE_bool(use_dwrf, false, "Use the DWRF writer instead of Alpha");
DEFINE_uint32(batch_size, 32, "Batch size to use when reading from file");
DEFINE_uint32(num_batches, 20, "Number of batches to read/write");
DEFINE_string(
    dictionary_array_names,
    "id_list_features",
    "Column names to write as dictionary array, semicolon separaated list");
DEFINE_bool(
    alpha_parallelize,
    true,
    "Use inter-writer parallelization. On by default");

using namespace ::facebook;

namespace {

class Writer {
 public:
  virtual ~Writer() = default;

  virtual void write(velox::VectorPtr& vector) = 0;
  virtual void close() = 0;
};

class DWRFWriter : public Writer {
 public:
  DWRFWriter(
      velox::memory::MemoryPool& rootPool,
      std::shared_ptr<velox::memory::MemoryPool> leafPool,
      // std::unique_ptr<velox::dwio::common::FileSink> fileSink,
      velox::TypePtr schema)
      : leafPool_(leafPool) {
    std::unique_ptr<velox::dwio::common::FileSink> writeSink;
    if (FLAGS_output_file == "") {
      writeSink = std::make_unique<velox::dwio::common::MemorySink>(
          1000 * 1024 * 1024,
          facebook::velox::dwio::common::FileSink::Options{
              .pool = leafPool_.get()});
    } else {
      auto writeFile =
          std::make_unique<velox::LocalWriteFile>(FLAGS_output_file);
      writeSink = std::make_unique<velox::dwio::common::WriteFileSink>(
          std::move(writeFile), FLAGS_output_file);
    }
    velox::dwrf::WriterOptions options;
    options.schema = schema;
    writer_ = std::make_unique<velox::dwrf::Writer>(
        std::move(options), std::move(writeSink), rootPool);
  }

  void write(velox::VectorPtr& vector) override {
    writer_->write(vector);
  }

  void close() override {
    writer_->close();
  }

 private:
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  std::unique_ptr<velox::dwrf::Writer> writer_;
}; // DWRFWriter

class AlphaWriter : public Writer {
 public:
  AlphaWriter(
      velox::memory::MemoryPool& memoryPool,
      std::shared_ptr<velox::memory::MemoryPool> leafPool,
      velox::TypePtr schema)
      : leafPool_(leafPool) {
    std::unique_ptr<velox::WriteFile> writeFile;
    if (FLAGS_output_file == "") {
      buffer_.reserve(512 * 1024 * 1024);
      auto inMemoryFile = std::make_unique<velox::InMemoryWriteFile>(&buffer_);
      writeFile = std::make_unique<dwio::api::BufferedWriteFile>(
          leafPool_, FLAGS_buffered_write_size, std::move(inMemoryFile));
    } else {
      writeFile = std::make_unique<velox::LocalWriteFile>(FLAGS_output_file);
    }
    alpha::VeloxWriterOptions options;
    std::vector<std::string> dictionaryArrays;
    folly::split(';', FLAGS_dictionary_array_names, dictionaryArrays);
    for (const auto& columnName : dictionaryArrays) {
      options.dictionaryArrayColumns.insert(columnName);
    }
    // if (FLAGS_alpha_parallelize) {
    //   options.parallelEncoding = true;
    //   options.parallelWriting = true;
    //   auto numThreads = std::thread::hardware_concurrency();
    //   if (FLAGS_threads > 0) {
    //     numThreads = FLAGS_threads;
    //   }
    //   options.parallelExecutor =
    //       std::make_shared<folly::CPUThreadPoolExecutor>(numThreads);
    // }
    writer_ = std::make_unique<alpha::VeloxWriter>(
        memoryPool, schema, std::move(writeFile), std::move(options));
  }

  void write(velox::VectorPtr& vector) override {
    writer_->write(vector);
  }

  void close() override {
    writer_->close();
  }

 private:
  std::string buffer_;
  std::shared_ptr<velox::memory::MemoryPool> leafPool_;
  std::unique_ptr<alpha::VeloxWriter> writer_;
}; // AlphaWriter
} // namespace

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};
  facebook::velox::memory::initializeMemoryManager(
      facebook::velox::memory::MemoryManagerOptions{});
  auto rootPool = facebook::velox::memory::memoryManager()->addRootPool(
      "velox_parallel_writer");
  auto leafPool = rootPool->addLeafChild("leaf_pool");
  auto accessDescriptor =
      facebook::dwio::common::request::AccessDescriptorBuilder{}
          .withClientId("parallel_writer")
          .build();
  auto inputStream = facebook::dwio::utils::InputStreamFactory::create(
      FLAGS_input_file, accessDescriptor);
  facebook::velox::dwio::common::ReaderOptions readerOpts{leafPool.get()};
  auto readerFactory = facebook::velox::alpha::AlphaReaderFactory();
  auto reader = readerFactory.createReader(
      std::make_unique<facebook::velox::dwio::common::BufferedInput>(
          std::move(inputStream), *leafPool),
      readerOpts);
  auto rowReader = reader->createRowReader();
  velox::VectorPtr schemaVector;
  rowReader->next(1, schemaVector);
  auto schema = schemaVector->type();
  std::vector<velox::VectorPtr> vectorsToWrite;
  vectorsToWrite.push_back(schemaVector);
  for (int i = 0; i < FLAGS_num_batches - 1; ++i) {
    velox::VectorPtr resultVector;
    auto read = rowReader->next(FLAGS_batch_size, resultVector);
    if (read == 0) {
      break;
    }
    vectorsToWrite.push_back(resultVector);
  }
  LOG(INFO) << "Read " << vectorsToWrite.size() << " batches";
  std::unique_ptr<Writer> writer;
  if (FLAGS_output_file == "") {
    LOG(INFO) << "No output file provided, writing to memory";
  }
  if (FLAGS_use_dwrf) {
    writer = std::make_unique<DWRFWriter>(*rootPool, leafPool, schema);
  } else {
    writer = std::make_unique<AlphaWriter>(*rootPool, leafPool, schema);
  }
  for (auto& vector : vectorsToWrite) {
    writer->write(vector);
  }
  writer->close();
  LOG(INFO) << "Wrote " << vectorsToWrite.size() << " batches while writing";

  return 0;
}
