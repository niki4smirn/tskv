#include "memtable.h"

#include <algorithm>
#include <memory>
#include <string>
#include "metric-storage/metric_storage.h"
#include "model/aggregations.h"
#include "model/column.h"
#include "model/model.h"

namespace tskv {

Memtable::Memtable(const Options& options, const MetricOptions& metric_options)
    : options_(options) {
  for (auto aggregation_type : metric_options.aggregation_types) {
    if (aggregation_type == AggregationType::kAvg) {
      columns_.push_back(
          CreateAggregatedColumn(ColumnType::kSum, options.bucket_inteval));
      columns_.push_back(
          CreateAggregatedColumn(ColumnType::kCount, options.bucket_inteval));
      continue;
    }
    auto column_type = ToColumnType(aggregation_type);
    columns_.push_back(
        CreateAggregatedColumn(column_type, options.bucket_inteval));
    assert(columns_.back()->GetType() == column_type);
  }
  if (options.store_raw) {
    columns_.push_back(CreateRawColumn(ColumnType::kRawTimestamps));
    columns_.push_back(CreateRawColumn(ColumnType::kRawValues));
  }
}

void Memtable::Write(const InputTimeSeries& time_series) {
  for (auto& column : columns_) {
    column->Write(time_series);
  }
  size_ += time_series.size();
}

Memtable::ReadResult Memtable::Read(
    const TimeRange& time_range, StoredAggregationType aggregation_type) const {
  auto column_type = ToColumnType(aggregation_type);
  if (column_type == ColumnType::kRawRead) {
    return ReadRawValues(time_range);
  }
  auto it = std::find_if(columns_.begin(), columns_.end(),
                         [column_type](const auto& column) {
                           return column->GetType() == column_type;
                         });
  assert(it != columns_.end());

  auto column = std::static_pointer_cast<IReadColumn>(*it);
  auto column_res = column->Read(time_range);

  if (!column_res) {
    return {.not_found = time_range};
  }

  std::optional<TimeRange> not_found;

  if (column_res->GetTimeRange().start > time_range.start) {
    not_found = TimeRange{time_range.start, column_res->GetTimeRange().start};
  }
  return {.found = column_res, .not_found = not_found};
}

Columns Memtable::ExtractColumns() {
  Columns res;
  for (auto& column : columns_) {
    res.push_back(column->Extract());
  }
  return res;
}

bool Memtable::NeedFlush() const {
  if (options_.capacity && size_ >= *options_.capacity) {
    return true;
  }

  auto ts_it = std::ranges::find_if(columns_, [](const auto& column) {
    return column->GetType() != ColumnType::kRawValues;
  });
  if (ts_it == columns_.end()) {
    return false;
  }
  const auto& ts_column = *ts_it;
  Duration age;
  if (ts_column->GetType() == ColumnType::kRawTimestamps) {
    auto raw_ts_column =
        std::dynamic_pointer_cast<RawTimestampsColumn>(ts_column);
    age = raw_ts_column->GetTimeRange().GetDuration();
  } else {
    auto ser_ts_column =
        std::dynamic_pointer_cast<ISerializableColumn>(ts_column);
    auto agg_ts_column =
        std::dynamic_pointer_cast<IAggregateColumn>(ser_ts_column);
    age = agg_ts_column->GetTimeRange().GetDuration();
  }
  if (options_.max_age && age >= *options_.max_age) {
    return true;
  }
  return false;
}

Memtable::ReadResult Memtable::ReadRawValues(
    const TimeRange& time_range) const {
  auto ts_it = std::ranges::find(columns_, ColumnType::kRawTimestamps,
                                 &IColumn::GetType);
  if (ts_it == columns_.end()) {
    return {.not_found = time_range};
  }
  auto vals_it =
      std::ranges::find(columns_, ColumnType::kRawValues, &IColumn::GetType);
  if (vals_it == columns_.end()) {
    return {.not_found = time_range};
  }
  Memtable::ReadResult result;

  auto ts_column = std::static_pointer_cast<RawTimestampsColumn>(*ts_it);
  auto vals_column = std::static_pointer_cast<RawValuesColumn>(*vals_it);
  auto column = std::make_shared<ReadRawColumn>(ts_column, vals_column);
  auto column_res = column->Read(time_range);

  if (!column_res) {
    return {.not_found = time_range};
  }

  std::optional<TimeRange> not_found;
  if (column_res->GetTimeRange().start > time_range.start) {
    not_found = TimeRange{time_range.start, column_res->GetTimeRange().start};
  }
  return {.found = column_res, .not_found = not_found};
}

}  // namespace tskv
