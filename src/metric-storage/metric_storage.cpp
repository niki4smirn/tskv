#include "metric_storage.h"

#include "model/column.h"
#include "model/model.h"

#include <cassert>
#include <iostream>
#include <ranges>

namespace tskv {

MetricStorage::MetricStorage(const Options& options)
    : memtable_(options.memtable_options, options.metric_options),
      persistent_storage_manager_(options.persistent_storage_manager_options) {}

Column MetricStorage::Read(const TimeRange& time_range,
                           AggregationType aggregation_type) const {
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
    auto columns = memtable_.ExtractColumns();
    SerializableColumns serializable_columns;
    serializable_columns.reserve(columns.size());
    for (auto& column : columns) {
      auto serializable_column =
          std::dynamic_pointer_cast<ISerializableColumn>(std::move(column));
      assert(serializable_column);
      serializable_columns.emplace_back(std::move(serializable_column));
    }
    persistent_storage_manager_.Write(serializable_columns);
  }
}

}  // namespace tskv
