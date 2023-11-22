#include "model/column.h"
#include "persistent-storage/disk_storage.h"
#include "persistent-storage/persistent_storage_manager.h"
#include "storage/storage.h"

#include <iostream>
#include <ranges>
#include <vector>

void print_reads(const tskv::Storage& storage, size_t metric_id,
                 const std::vector<tskv::TimeRange>& time_ranges) {
  for (auto time_range : time_ranges) {
    auto column =
        storage.Read(metric_id, time_range, tskv::AggregationType::kSum);
    auto read_col = std::dynamic_pointer_cast<tskv::IReadColumn>(column);
    auto read_time_range = read_col->GetTimeRange();
    auto values = read_col->GetValues();
    auto step = (read_time_range.end - read_time_range.start) / values.size();
    for (size_t i = 0; i < values.size(); ++i) {
      std::cout << read_time_range.start + i * step << " " << values[i]
                << std::endl;
    }
    std::cout << std::endl;

    column = storage.Read(metric_id, time_range, tskv::AggregationType::kNone);
    if (!column) {
      std::cout << "not found" << std::endl << std::endl;
      continue;
    }
    auto read_raw_col = std::dynamic_pointer_cast<tskv::ReadRawColumn>(column);
    for (size_t i = 0; i < read_raw_col->GetTimestamps().size(); ++i) {
      std::cout << read_raw_col->GetTimestamps()[i] << " "
                << read_raw_col->GetValues()[i] << std::endl;
    }
    std::cout << std::endl;
  }
}

int main() {
  tskv::Storage storage;
  auto metric_id = storage.InitMetric(tskv::MetricStorage::Options{
      tskv::MetricOptions{
          {tskv::AggregationType::kSum},
          // {},
      },
      tskv::Memtable::Options{
          .bucket_inteval = 1, .capacity = 4, .store_raw = true},
      tskv::PersistentStorageManager::Options{
          .levels =
              {
                  {.bucket_interval = 1,
                   .level_duration = 6,
                   .store_raw = true},
                  {.bucket_interval = 2,
                   .level_duration = 10,
                   .store_raw = false},
              },
          .storage = std::make_shared<tskv::DiskStorage>(
              tskv::DiskStorage::Options{"./tmp/tskv"})}});

  storage.Write(metric_id, tskv::InputTimeSeries{{1, 1}, {2, 2}, {2, 1}});
  print_reads(storage, metric_id, {{2, 3}, {1, 5}});
  storage.Write(metric_id, tskv::InputTimeSeries{
                               {2, 3}, {3, 1}, {3, 10}, {4, 2}, {4, -1}});
  print_reads(storage, metric_id, {{2, 3}, {1, 5}});

  storage.Write(metric_id, tskv::InputTimeSeries{{6, 1}, {8, 2}, {10, 3}});
  print_reads(storage, metric_id, {{2, 3}, {1, 5}, {1, 11}});
  return 0;
}
