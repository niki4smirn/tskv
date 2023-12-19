#pragma once

#include "metric-storage/metric_storage.h"
#include "model/model.h"

#include <unordered_map>

namespace tskv {

using MetricId = uint64_t;

class Storage {
 public:
  MetricId InitMetric(const MetricStorage::Options& options);
  Column Read(MetricId metric_id, const TimeRange& time_range,
              AggregationType aggregation_type) const;

  void Write(MetricId metric_id, const InputTimeSeries& time_series);
  void Flush();

 private:
  std::unordered_map<MetricId, MetricStorage> metrics_;
  size_t next_id_ = 0;
};

}  // namespace tskv
