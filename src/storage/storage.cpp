#include "storage.h"

namespace tskv {

MetricId Storage::InitMetric(const MetricStorage::Options& options) {
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
                      AggregationType aggregation_type) {
  auto it = metrics_.find(id);
  if (it == metrics_.end()) {
    throw std::runtime_error("Metric with id " + std::to_string(id) +
                             " not found");
  }
  return it->second.Read(time_range, aggregation_type);
}

}  // namespace tskv
