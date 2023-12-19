// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Singleton.h>
#include "common/init/light.h"
#include "common/strings/URL.h"
#include "data_preproc/logging/ManifoldLogger.h"
#include "glog/logging.h"
#include "koski/connect/dw/dw.h"
#include "koski/connect/dw/dwio/DwioTable.h"
#include "koski/core/TableScanOp.h"
#include "koski/cpp_dataframe/Dataframe.h"
#include "velox/common/base/StatsReporter.h"

using namespace ::facebook;

DEFINE_string(ns, "", "Namespace to read from");
DEFINE_string(table, "", "Table to read from");
DEFINE_string(
    partition,
    "ds=2022-04-23/pipeline=ctr_mbl_feed_model_opt_out_lcc4_remove_recall_clustered_20_downsample",
    "Partition to use when reading data");
DEFINE_string(session_id, "", "DPP session id to modify");
DEFINE_string(new_session_id, "", "DPP session id to upload");
DEFINE_bool(overwrite, false, "Overwrite new session, if exists.");

std::shared_ptr<const koski::core::IOp> getOp(
    const std::shared_ptr<const koski::core::IOp> op,
    const std::shared_ptr<koski::core::SessionCtx>& ctx) {
  std::shared_ptr<const koski::core::IOp> prev = nullptr;
  if (auto ts = dynamic_cast<const koski::core::TableScanOp*>(op.get())) {
    auto table = koski::dw::getTable(
        {
            .ns = FLAGS_ns,
            .table = FLAGS_table,
            .partitionsOnly = false,
        },
        ctx);
    return std::make_shared<koski::core::TableScanOp>(
        std::move(table), ts->getCondition());
  } else {
    return op->withInputs({getOp(op->getInput(), ctx)});
  }
}

std::vector<std::tuple<std::string, std::string>> getPartitionPairs(
    const std::string& partition) {
  std::vector<std::string> segments;
  folly::split('/', partition, segments);

  std::vector<std::tuple<std::string, std::string>> partitionPairs(
      segments.size());
  for (auto i = 0; i < segments.size(); ++i) {
    std::vector<std::string> parts;
    folly::split('=', segments[i], parts);
    assert(parts.size() == 2);
    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    partitionPairs[i] = {parts[0], strings::URL::urlUnescape(parts[1])};
  }

  return partitionPairs;
}

void traverseDataframeNodes(
    folly::dynamic& obj,
    const std::vector<std::tuple<std::string, std::string>>& partitionPairs) {
  if (obj.isObject()) {
    folly::dynamic_view view{obj};
    if (view.descend("name").string_or("") == "CallExpr") {
      auto inputs = view.descend("inputs").value_or(nullptr);
      if (!inputs.isNull()) {
        if (inputs.size() == 2) {
          folly::dynamic_view fieldView{inputs[0]};
          if (fieldView.descend("name").string_or("") == "FieldAccessExpr") {
            for (const auto& segment : partitionPairs) {
              if (fieldView.descend("fieldName").string_or("") ==
                  std::get<0>(segment)) {
                auto value = inputs[1]["value"]["value"].asString();
                if (value != std::get<1>(segment)) {
                  LOG(INFO) << "Setting " << std::get<0>(segment) << " to "
                            << std::get<1>(segment) << ". Was " << value;
                  obj["inputs"][1]["value"]["value"] = std::get<1>(segment);
                }
              }
            }
          }
        }
      }
    }
    for (auto& child : obj.items()) {
      traverseDataframeNodes(child.second, partitionPairs);
    }
  } else if (obj.isArray()) {
    for (auto i = 0; i < obj.size(); ++i) {
      traverseDataframeNodes(obj[i], partitionPairs);
    }
  }
}

std::string replacePartitionValues(
    const std::string& serializedDataframe,
    const std::vector<std::tuple<std::string, std::string>>& partitionPairs) {
  auto obj =
      folly::parseJson(serializedDataframe, velox::getSerializationOptions());

  traverseDataframeNodes(obj, partitionPairs);

  return folly::json::serialize(obj, velox::getSerializationOptions());
}

int main(int argc, char* argv[]) {
  auto init = facebook::init::InitFacebookLight{&argc, &argv};

  if (FLAGS_ns.empty()) {
    LOG(ERROR) << "Namespace argument is required.";
    return 1;
  }

  if (FLAGS_table.empty()) {
    LOG(ERROR) << "Table argument is required.";
    return 1;
  }

  if (FLAGS_partition.empty()) {
    LOG(ERROR) << "Partition argument is required.";
    return 1;
  }

  auto sessionId = folly::trimWhitespace(FLAGS_session_id).str();
  auto newSessionId = folly::trimWhitespace(FLAGS_new_session_id).str();

  if (sessionId.empty()) {
    LOG(ERROR) << "Session id argument is required.";
    return 1;
  }

  if (newSessionId.empty()) {
    LOG(ERROR) << "New session id argument is required.";
    return 1;
  }

  if (sessionId == newSessionId) {
    LOG(ERROR) << "New session id cannot be the same as original session id.";
    return 1;
  }

  data_preproc::logging::ManifoldLogger manifoldLogger;
  if (manifoldLogger.sessionAlreadyExists(newSessionId)) {
    if (FLAGS_overwrite) {
      manifoldLogger.deleteSession(newSessionId);
    } else {
      LOG(ERROR) << "New session id already exists.";
      return 1;
    }
  }

  auto partitionPairs = getPartitionPairs(FLAGS_partition);

  auto sessionRequest = manifoldLogger.fetchCreateSessionRequest(sessionId);

  auto serialized = replacePartitionValues(
      sessionRequest.dataset_ref()
          ->get_koskiDataset()
          .get_serializedDataframe(),
      partitionPairs);
  auto df = koski::deserializeDataframe(serialized);
  auto ops = getOp(df->getOp(), df->getCtx());
  df = koski::Dataframe::createFromOp(ops, df->getCtx());
  sessionRequest.dataset_ref()
      ->koskiDataset_ref()
      ->serializedDataframe_ref()
      .emplace(df->serializeDataframe());

  manifoldLogger.logCreateSessionRequestToManifold(
      sessionRequest, newSessionId);
}

// Initialize dummy Velox stats reporter
folly::Singleton<velox::BaseStatsReporter> reporter([]() {
  return new velox::DummyStatsReporter();
});
