#include "metric_storage.h"

#include "model/column.h"
#include "model/model.h"

#include <cassert>
#include <ranges>

namespace tskv {

MetricStorage::MetricStorage(const Options& options)
    : memtable_(options.memtable_options, options.metric_options),
      persistent_storage_manager_(options.persistent_storage) {}

Column MetricStorage::Read(const TimeRange& time_range,
                           AggregationType aggregation_type) {
  if (aggregation_type == AggregationType::kAvg) {
    // TODO: implement
    assert(false);
  }

  auto stored_aggregation = ToStoredAggregationType(aggregation_type);
  auto [found, not_found] = memtable_.Read(time_range, stored_aggregation);

  Column column;
  if (not_found) {
    column = persistent_storage_manager_.Read(*not_found, stored_aggregation);
  }

  Column result = column;
  if (!result) {
    result = found;
  } else {
    result->Merge(found);
  }
  return result;
}

void MetricStorage::Write(const InputTimeSeries& time_series) {
  memtable_.Write(time_series);

  if (memtable_.NeedFlush()) {
    persistent_storage_manager_.Write(memtable_.ExtractColumns());
  }
}

}  // namespace tskv
