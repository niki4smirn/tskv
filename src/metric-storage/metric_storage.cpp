#include "metric_storage.h"

#include "model/column.h"
#include "model/model.h"

#include <cassert>
#include <ranges>

namespace tskv {

MetricStorage::MetricStorage(const Options& options)
    : cache_(options.cache_options),
      memtable_(options.memtable_options, options.metric_options),
      persistent_storage_manager_(options.persistent_storage) {}

Column MetricStorage::Read(const TimeRange& time_range,
                           AggregationType aggregation_type) {
  // TODO: use cache
  if (aggregation_type == AggregationType::kAvg) {
    // TODO: implement
    assert(false);
  }
  auto [found, not_found] =
      memtable_.Read({time_range}, ToStoredAggregationType(aggregation_type));
  auto columns = persistent_storage_manager_.Read(not_found, aggregation_type);

  // merge iterates over all columns in chronological order
  Column result;
  if (!columns.empty()) {
    result = columns.front();
    for (auto column : columns | std::views::drop(1)) {
      result->Merge(column);
    }
  }
  if (!found.empty()) {
    int to_drop = 0;
    if (!result) {
      result = found.front();
      to_drop = 1;
    }
    for (const auto& column : found | std::views::drop(to_drop)) {
      result->Merge(column);
    }
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
