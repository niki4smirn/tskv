#include "memtable.h"

#include <algorithm>
#include <memory>
#include <string>
#include "metric-storage/metric_storage.h"
#include "model/column.h"
#include "model/model.h"

namespace tskv {

Memtable::Memtable(const Options& options, const MetricOptions& metric_options)
    : options_(options) {
  // TODO: maybe prevent passing raw_timestamps and raw_values?
  for (auto column_type : metric_options.column_types) {
    if (column_type == ColumnType::kRawRead) {
      columns_.push_back(CreateRawColumn(ColumnType::kRawTimestamps));
      columns_.push_back(CreateRawColumn(ColumnType::kRawValues));
    } else {
      columns_.push_back(CreateColumn(column_type, options.bucket_inteval));
    }
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
  auto column_type = static_cast<ColumnType>(aggregation_type);
  auto it = std::find_if(columns_.begin(), columns_.end(),
                         [column_type](const auto& column) {
                           return column->GetType() == column_type;
                         });
  if (it == columns_.end()) {
    return ReadRawValues(time_range, aggregation_type);
  }

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
  return size_ >= options_.capacity;
}

Memtable::ReadResult Memtable::ReadRawValues(
    const TimeRange& time_range, StoredAggregationType aggregation_type) const {
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
