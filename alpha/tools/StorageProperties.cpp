// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <glog/logging.h>
#include <fstream>

#include "common/init/light.h"
#include "dwio/catalog/Catalog.h"
#include "dwio/catalog/fbhive/HiveCatalog.h"
#include "dwio/catalog/impl/DefaultCatalog.h"

DEFINE_string(ns, "", "Namespace to read from.");
DEFINE_string(table, "", "Table to read from.");
DEFINE_string(output, "", "Output file to write");

using namespace ::facebook;

struct PartitionValuesHash {
  std::size_t operator()(const std::vector<std::string>& v) const {
    return folly::hash::hash_range(v.cbegin(), v.cend());
  }
};

struct PartitionValuesEqual {
  bool operator()(
      const std::vector<std::string>& lhs,
      const std::vector<std::string>& rhs) const {
    if (lhs.size() != rhs.size()) {
      return false;
    }

    for (auto i = 0; i < lhs.size(); ++i) {
      if (lhs[i] != rhs[i]) {
        return false;
      }
    }

    return true;
  }
};

std::string tab(uint32_t t) {
  return std::string(t * 4, ' ');
}

int main(int argc, char* argv[]) {
  facebook::init::initFacebookLight(&argc, &argv);

  auto ns = FLAGS_ns;
  auto table = FLAGS_table;

  dwio::common::request::AccessDescriptor ad =
      dwio::common::request::AccessDescriptorBuilder{}
          .withClientId("alpha.storage.properties")
          .withNamespace(ns)
          .withTable(table)
          .build();

  dwio::catalog::fbhive::HiveCatalog catalog{ad};

  if (!catalog.existsTable(ns, table)) {
    LOG(INFO) << "Table doesn't exist.";
    return 1;
  }

  auto tableProperties = catalog.getTableStorageProperties(ns, table);
  auto partitionKeys = tableProperties->getPartitionKeys();

  std::unordered_set<
      std::vector<std::string>,
      PartitionValuesHash,
      PartitionValuesEqual>
      partitionValues;
  auto partitions = catalog.getPartitionNamesByFilter(ns, table, {}, -1);
  for (const auto& partition : partitions) {
    std::vector<std::string> segments;
    folly::split('/', partition, segments);

    std::unordered_map<std::string, std::string> partitionPairs(
        segments.size());
    for (auto i = 0; i < segments.size(); ++i) {
      std::vector<std::string> pair;
      folly::split('=', segments[i], pair);
      if (pair.size() != 2) {
        LOG(INFO) << "Partition segments must be in the format key=value.";
        return 1;
      }

      partitionPairs.insert({pair[0], pair[1]});
    }

    std::vector<std::string> values;
    for (const auto& key : partitionKeys) {
      auto it = partitionPairs.find(key);
      if (it == partitionPairs.end()) {
        LOG(INFO) << "Unable to find partition key '" << key
                  << "' in partition '" << partition << "'";
        return 1;
      }
      values.push_back(it->second);
    }

    partitionValues.insert(std::move(values));
  }

  std::ofstream output{
      FLAGS_output, std::ios::out | std::ios::binary | std::ios ::trunc};

  if (!partitionValues.empty()) {
    output << "\"" << ns << "\": NamespaceStorageProperties(" << std::endl;
    output << tab(1) << "table_name_to_properties={" << std::endl;
    output << tab(2) << "\"" << table << "\": TableStorageProperties("
           << std::endl;
    output << tab(3) << "sub_partition_name_to_properties={" << std::endl;

    for (const auto& values : partitionValues) {
      std::shared_ptr<
          const dwio::catalog::fbhive::HivePartitionStorageProperties>
          partitionProperties;
      try {
        partitionProperties =
            catalog.getPartitionStorageProperties(ns, table, values);
      } catch (const velox::VeloxException& e) {
        LOG(INFO) << "Skipping " << folly::join(",", values) << "...";
      }

      if (partitionProperties) {
        auto source = partitionProperties->getFeatureOrdering();
        if (!source.featureOrdering_ref()->empty()) {
          output << tab(4) << "\"";
          for (auto i = 0; i < partitionKeys.size(); ++i) {
            output << partitionKeys[i] << "=" << values[i];
            if (i < partitionKeys.size() - 1) {
              output << "/";
            }
          }
          output << "\": [" << std::endl;
          output << tab(5) << "StorageProperty(" << std::endl;
          output << tab(6) << "stream_ordering=StreamOrdering(" << std::endl;
          output << tab(7) << "stream_order_list=[" << std::endl;

          std::vector<std::tuple<size_t, std::vector<int64_t>>>
              featureReordering;
          featureReordering.reserve(source.featureOrdering_ref()->size());
          for (const auto& column : source.featureOrdering_ref().value()) {
            output << tab(8) << "StreamOrder(" << std::endl;
            output << tab(9) << "column_logical_id="
                   << column.columnId_ref()->columnIdentifier_ref().value()
                   << "," << std::endl;
            output << tab(9) << "stream_order=[" << std::endl;
            for (const auto& feature : column.featureOrder_ref().value()) {
              output << tab(10) << feature << "," << std::endl;
            }
            output << tab(9) << "]," << std::endl;
            output << tab(8) << ")," << std::endl;
          }
          output << tab(7) << "]" << std::endl;
          output << tab(6) << ")" << std::endl;
          output << tab(5) << ")" << std::endl;
          output << tab(4) << "]," << std::endl;
        }
      }
    }
    output << tab(3) << "}" << std::endl;
    output << tab(2) << ")," << std::endl;
    output << tab(1) << "}" << std::endl;
    output << ")" << std::endl;
  }
}
