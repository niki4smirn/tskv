#include "metric-storage/metric_storage.h"
#include "model/aggregations.h"
#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/disk_storage.h"
#include "persistent-storage/persistent_storage_manager.h"
#include "storage/storage.h"

#include <fstream>
#include <iostream>
#include <ranges>
#include <vector>

std::vector<std::string> Split(const std::string& s,
                               const std::string& delimiter) {
  size_t pos_start = 0, pos_end, delim_len = delimiter.length();
  std::string token;
  std::vector<std::string> res;

  while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
    token = s.substr(pos_start, pos_end - pos_start);
    pos_start = pos_end + delim_len;
    res.push_back(token);
  }

  res.push_back(s.substr(pos_start));
  return res;
}

int main() {
  std::ifstream input("../../test_data/timescaledb-data");
  std::string line;
  getline(input, line);
  std::unordered_map<std::string, std::vector<std::string>> metric_names;
  while (getline(input, line)) {
    if (line.empty()) {
      break;
    }
    auto names = Split(line, ",");
    metric_names[names[0]] =
        std::vector<std::string>(names.begin() + 1, names.end());
  }
  constexpr uint64_t kMb = 1024 * 1024;
  constexpr uint64_t kBufferSize = 1 * kMb;
  std::unordered_map<size_t, std::vector<tskv::InputTimeSeries>> time_series;
  while (getline(input, line)) {
    auto tags = line;
    assert(tags.starts_with("tags,"));
    getline(input, line);
    auto metrics = Split(line, ",");
    auto metric_type = metrics[0];
    const auto& cur_metric_names = metric_names[metric_type];
    assert(cur_metric_names.size() == metrics.size() - 2);
    tskv::TimePoint timestamp = std::strtoull(metrics[1].c_str(), nullptr, 10);
    for (int i = 2; i < metrics.size(); ++i) {
      auto hash = std::hash<std::string>{}(tags + ',' + metric_type + ',' +
                                           cur_metric_names[i - 2]);
      tskv::Value metric_value = std::strtod(metrics[i].c_str(), nullptr);
      if (time_series.find(hash) == time_series.end()) {
        time_series[hash] = std::vector<tskv::InputTimeSeries>();
      }
      if (time_series[hash].empty() ||
          time_series[hash].back().size() * sizeof(tskv::Record) >=
              kBufferSize) {
        time_series[hash].emplace_back();
      }
      time_series[hash].back().emplace_back(timestamp, metric_value);
    }
  }
  input.close();

  std::cerr << "read " << time_series.size() << " time series" << std::endl;

  tskv::Storage storage;
  tskv::MetricStorage::Options default_options = {
      tskv::MetricOptions{
          {tskv::StoredAggregationType::kSum,
           tskv::StoredAggregationType::kCount,
           tskv::StoredAggregationType::kMin,
           tskv::StoredAggregationType::kMax},
      },
      tskv::Memtable::Options{
          .bucket_inteval = tskv::Duration::Seconds(40),
          .max_size = 1000 * kMb,
          .max_age = tskv::Duration::Days(1),
          .store_raw = true,
      },
      tskv::PersistentStorageManager::Options{
          .levels = {{
                         .bucket_interval = tskv::Duration::Seconds(40),
                         .level_duration = tskv::Duration::Minutes(10),
                         .store_raw = true,
                     },
                     {
                         .bucket_interval = tskv::Duration::Minutes(30),
                         .level_duration = tskv::Duration::Days(1),
                     },
                     {
                         .bucket_interval = tskv::Duration::Hours(3),
                         .level_duration = tskv::Duration::Months(1),
                     }},
          .storage = std::make_unique<tskv::DiskStorage>(
              tskv::DiskStorage::Options{.path = "./tmp/tskv"}),
      },
  };

  std::unordered_map<size_t, tskv::MetricId> metric_ids;
  for (const auto& [hash, _] : time_series) {
    metric_ids[hash] = storage.InitMetric(default_options);
  }

  int idx = 0;
  while (true) {
    bool wrote = false;

    for (auto& [hash, time_series] : time_series) {
      if (idx >= time_series.size()) {
        continue;
      }
      auto& cur_time_series = time_series[idx];
      std::cerr << "writing " << cur_time_series.size() << " records for "
                << hash << std::endl;
      storage.Write(metric_ids[hash], cur_time_series);
      wrote = true;
    }

    if (!wrote) {
      break;
    }
  }
  return 0;
}
