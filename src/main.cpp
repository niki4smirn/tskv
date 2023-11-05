#include "model/column.h"
#include "persistent-storage/disk_storage.h"
#include "storage/storage.h"

#include <iostream>
#include <ranges>
#include <vector>

int main() {
  tskv::Storage storage;
  auto metric_id = storage.InitMetric(tskv::MetricStorage::Options{
      tskv::MetricOptions{
          {tskv::ColumnType::kRawTimestamps, tskv::ColumnType::kRawValues}},
          // {tskv::ColumnType::kSum}},
      tskv::Cache::Options{.capacity = 1000},
      tskv::Memtable::Options{.bucket_inteval = 1, .capacity = 5},
      std::make_shared<tskv::DiskStorage>(
          tskv::DiskStorage::Options{"./tmp/tskv", true})});
  storage.Write(metric_id,
                tskv::InputTimeSeries{
                    {1, 1}, {2, 2}, {2, 1}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});

  auto column = storage.Read(metric_id, tskv::TimeRange{1, 5},
                             tskv::AggregationType::kSum);
  if (std::dynamic_pointer_cast<tskv::ReadRawColumn>(column)) {
    auto read_raw_col = std::dynamic_pointer_cast<tskv::ReadRawColumn>(column);
    for (size_t i = 0; i < read_raw_col->GetTimestamps().size(); ++i) {
      std::cout << read_raw_col->GetTimestamps()[i] << " "
                << read_raw_col->GetValues()[i] << std::endl;
    }
  } else {
    for (auto val : column->GetValues()) {
      std::cout << val << " ";
    }
  }
  return 0;
}
