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

  // should somehow return error (for example, when there is no free space in
  // persistent storage)
  //
  // Still not sure about error handling pattern. Probably std::expected is the
  // best choice for the project (I hope it's not a problem, that it was intoduced
  // only in C++23)
  void Write(MetricId metric_id, const InputTimeSeries& time_series);

 private:
  std::unordered_map<MetricId, MetricStorage> metrics_;
  size_t next_id_ = 0;
};

}  // namespace tskv
