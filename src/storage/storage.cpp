#include "storage.h"
#include "model/aggregations.h"

namespace tskv {

void ValidateOptions(const MetricStorage::Options& options) {
  auto memtable_options = options.memtable_options;
  auto persistent_storage_options = options.persistent_storage_manager_options;
  for (auto aggregation_type : options.metric_options.aggregation_types) {
    if (aggregation_type == StoredAggregationType::kNone) {
      throw std::runtime_error("Aggregation cannot be none");
    }
  }

  if (!memtable_options.max_bytes_size && !memtable_options.max_age) {
    throw std::runtime_error("Memtable should have max_size or max_age");
  }

  if (!persistent_storage_options.levels.empty()) {
    if (persistent_storage_options.levels[0].bucket_interval !=
        memtable_options.bucket_interval) {
      throw std::runtime_error(
          "First level bucket interval should be equal to memtable bucket "
          "interval");
    }
  }

  for (size_t i = 1; i < persistent_storage_options.levels.size(); ++i) {
    if (persistent_storage_options.levels[i].bucket_interval %
            persistent_storage_options.levels[i - 1].bucket_interval !=
        0) {
      throw std::runtime_error(
          "Bucket intervals should be multiples of each other");
    }
  }

  if (persistent_storage_options.levels[0].store_raw &&
      !memtable_options.store_raw) {
    throw std::runtime_error("We can store raw values only for some prefix");
  }
  for (size_t i = 1; i < persistent_storage_options.levels.size(); ++i) {
    if (persistent_storage_options.levels[i].store_raw &&
        !persistent_storage_options.levels[i - 1].store_raw) {
      throw std::runtime_error("We can store raw values only for some prefix");
    }
  }
}

MetricId Storage::InitMetric(const MetricStorage::Options& options) {
  ValidateOptions(options);
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

void Storage::Flush() {
  for (auto& [_, metric] : metrics_) {
    metric.Flush();
  }
}

}  // namespace tskv
