#include "memtable.h"

#include <algorithm>
#include <string>
#include "metric-storage/metric_storage.h"
#include "model/column.h"

namespace tskv {

Memtable::Memtable(const Options& options, const MetricOptions& metric_options)
    : options_(options) {
  for (auto column_type : metric_options.column_types) {
    columns_.push_back(CreateColumn(column_type, options.bucket_inteval));
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
  auto it = std::find_if(
      columns_.begin(), columns_.end(),
      [](const auto& column) { return column->GetType() == ColumnType::kSum; });
  if (it == columns_.end()) {
    throw std::runtime_error("Column not found");
  }

  Memtable::ReadResult result;
  for (const auto& time_range : time_ranges) {
    auto& column = *it;
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

}  // namespace tskv
