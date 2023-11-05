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
  for (auto column_type : metric_options.column_types) {
    if (column_type == ColumnType::kRawTimestamps ||
        column_type == ColumnType::kRawValues) {
      columns_.push_back(CreateRawColumn(column_type));
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

Memtable::ReadResult Memtable::Read(const std::vector<TimeRange>& time_ranges,
                                    StoredAggregationType aggregation_type) {
  auto column_type = static_cast<ColumnType>(aggregation_type);
  auto it = std::find_if(columns_.begin(), columns_.end(),
                         [column_type](const auto& column) {
                           return column->GetType() == column_type;
                         });
  if (it == columns_.end()) {
    return ReadRawValues(time_ranges, aggregation_type);
  }

  Memtable::ReadResult result;
  for (const auto& time_range : time_ranges) {
    auto column = std::static_pointer_cast<IReadColumn>(*it);
    auto column_res = column->Read(time_range);
    if (column_res) {
      result.found.push_back(std::move(column_res));
    } else {
      result.not_found.push_back(time_range);
    }
  }
  return result;
}

Columns Memtable::ExtractColumns() {
  auto res = std::move(columns_);
  columns_ = {};
  return res;
}

bool Memtable::NeedFlush() const {
  return size_ >= options_.capacity;
}

Memtable::ReadResult Memtable::ReadRawValues(
    const std::vector<TimeRange>& time_ranges,
    StoredAggregationType aggregation_type) {
  auto ts_it = std::ranges::find(columns_, ColumnType::kRawTimestamps,
                                 &IColumn::GetType);
  if (ts_it == columns_.end()) {
    return {.not_found = time_ranges};
  }
  auto vals_it =
      std::ranges::find(columns_, ColumnType::kRawValues, &IColumn::GetType);
  if (vals_it == columns_.end()) {
    return {.not_found = time_ranges};
  }
  Memtable::ReadResult result;
  for (const auto& time_range : time_ranges) {
    auto ts_column = std::static_pointer_cast<RawTimestampsColumn>(*ts_it);
    auto vals_column = std::static_pointer_cast<RawValuesColumn>(*vals_it);
    auto ts_values = ts_column->GetValues();
    auto column = std::make_shared<ReadRawColumn>(
        std::vector<TimePoint>(ts_values.begin(), ts_values.end()),
        vals_column->GetValues());
    auto column_res = column->Read(time_range);
    if (column_res) {
      result.found.push_back(std::move(column_res));
    } else {
      result.not_found.push_back(time_range);
    }
  }
  return result;
}

}  // namespace tskv
