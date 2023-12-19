// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <glog/logging.h>
#include "common/init/light.h"
#include "data_preproc/client/DataClient.h"
#include "data_preproc/client/SessionClient.h"
#include "data_preproc/common/CommonFlags.h"
#include "data_preproc/common/SessionConfigUtil.h"
#include "data_preproc/integration_tests/common/LocalCluster.h"
#include "data_preproc/master/MasterFlags.h"
#include "data_preproc/server/WorkerFlags.h"

using namespace ::facebook;

DEFINE_string(
    session_id,
    "alpha.shadow.3646CCDD617BA9D029BC29958C137E81",
    "Existing DPP session id.");
DEFINE_uint32(worker_count, 1, "Number of DPP workers.");
DEFINE_uint32(worker_thread_count, 1, "Number of threads in each DPP worker.");

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};
  auto [dataset, plan] = data_preproc::session_config_util::
      getDatasetAndProcessingPlanFromSessionId(FLAGS_session_id);

  data_preproc::MasterFlags masterFlags;
  data_preproc::WorkerFlags workerFlags;
  data_preproc::CommonFlags commonFlags;

  workerFlags.worker_num_threads = FLAGS_worker_thread_count;

  auto localCluster = std::make_unique<data_preproc::test_utils::LocalCluster>(
      masterFlags, workerFlags, commonFlags, FLAGS_worker_count);
  auto sessionClient = localCluster->getSessionClient();
  auto dataClient = localCluster->getDataClient(1);

  auto sessionId = sessionClient->syncCreateSession(dataset, plan);
  LOG(INFO) << "Session ID: " << sessionId;

  size_t count = 0;
  dataClient->joinSession(sessionId);
  while (dataClient->getNextBatch() != folly::none) {
    ++count;
  }
  LOG(INFO) << "Loaded " << count << " batches.";
}
