#include "storage.h"

namespace tskv {

bool ValidateOptions(const MetricStorage::Options& options) {
  auto memtable_options = options.memtable_options;
  auto persistent_storage_options = options.persistent_storage_manager_options;
  for (auto aggregation_type : options.metric_options.aggregation_types) {
    if (aggregation_type == AggregationType::kNone) {
      return false;
    }
  }

  // bucket intervals should be mulitples of each other
  if (!persistent_storage_options.levels.empty() &&
      persistent_storage_options.levels[0].bucket_interval %
              memtable_options.bucket_inteval !=
          0) {
    return false;
  }
  for (size_t i = 1; i < persistent_storage_options.levels.size(); ++i) {
    if (persistent_storage_options.levels[i].bucket_interval %
            persistent_storage_options.levels[i - 1].bucket_interval !=
        0) {
      return false;
    }
  }

  // we can store raw values only for some prefix
  if (persistent_storage_options.levels[0].store_raw &&
      !memtable_options.store_raw) {
    return false;
  }
  for (size_t i = 1; i < persistent_storage_options.levels.size(); ++i) {
    if (persistent_storage_options.levels[i].store_raw &&
        !persistent_storage_options.levels[i - 1].store_raw) {
      return false;
    }
  }

  return true;
}

MetricId Storage::InitMetric(const MetricStorage::Options& options) {
  if (!ValidateOptions(options)) {
    throw std::runtime_error("Invalid metric options");
  }
  MetricId id = next_id_++;
  metrics_.emplace(id, options);
  return id;
}

void Storage::Write(MetricId id, const InputTimeSeries& input) {
  auto it = metrics_.find(id);
  if (it == metrics_.end()) {
    throw std::runtime_error("Metric with id " + std::to_string(id) +
                             " not found");
  }
  it->second.Write(input);
}

Column Storage::Read(MetricId id, const TimeRange& time_range,
                     AggregationType aggregation_type) const {
  auto it = metrics_.find(id);
  if (it == metrics_.end()) {
    throw std::runtime_error("Metric with id " + std::to_string(id) +
                             " not found");
  }
  return it->second.Read(time_range, aggregation_type);
}

}  // namespace tskv
