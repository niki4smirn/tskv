#include "metric-storage/metric_storage.h"
#include "model/aggregations.h"
#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/disk_storage.h"
#include "persistent-storage/persistent_storage_manager.h"
#include "storage/storage.h"

#include <fstream>
#include <iostream>
#include <random>
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

struct WriteResult {
  tskv::TimeRange time_range;
  std::vector<tskv::MetricId> metrid_ids;
};

WriteResult Write(tskv::Storage& storage) {
  auto start = std::chrono::steady_clock::now();
  std::ifstream input("../../test_data/timescaledb-data-5-1s");
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
  std::optional<tskv::TimePoint> min;
  std::optional<tskv::TimePoint> max;
  while (getline(input, line)) {
    auto tags = line;
    assert(tags.starts_with("tags,"));
    getline(input, line);
    auto metrics = Split(line, ",");
    auto metric_type = metrics[0];
    const auto& cur_metric_names = metric_names[metric_type];
    assert(cur_metric_names.size() == metrics.size() - 2);
    // fast nanoseconds to microseconds conversion
    metrics[1][metrics[1].size() - 3] = 0;
    tskv::TimePoint timestamp = std::strtoull(metrics[1].c_str(), nullptr, 10);
    if (!min) {
      min = timestamp;
    } else {
      min = std::min(*min, timestamp);
    }
    if (!max) {
      max = timestamp;
    } else {
      max = std::max(*max, timestamp);
    }
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

  auto end = std::chrono::steady_clock::now();

  std::cerr << "read time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << "ms" << std::endl;

  std::cerr << "starting write" << std::endl;
  start = std::chrono::steady_clock::now();

  tskv::MetricStorage::Options default_options = {
      tskv::MetricOptions{
          {tskv::StoredAggregationType::kSum,
           tskv::StoredAggregationType::kCount,
           tskv::StoredAggregationType::kMin,
           tskv::StoredAggregationType::kMax},
      },
      tskv::Memtable::Options{
          .bucket_interval = tskv::Duration::Seconds(10),
          .max_bytes_size = 1000 * kMb,
          .max_age = tskv::Duration::Hours(9),
          .store_raw = true,
      },
      tskv::PersistentStorageManager::Options{
          .levels = {{
                         .bucket_interval = tskv::Duration::Seconds(10),
                         .level_duration = tskv::Duration::Hours(20),
                         .store_raw = true,
                     },
                     {
                         .bucket_interval = tskv::Duration::Minutes(2),
                         .level_duration = tskv::Duration::Weeks(2),
                     }},
          .storage =
              std::make_unique<tskv::DiskStorage>(tskv::DiskStorage::Options{
                  .path = "./tmp/tskv",
              }),
      },
  };

  std::unordered_map<size_t, tskv::MetricId> metric_ids;
  for (const auto& [hash, _] : time_series) {
    metric_ids[hash] = storage.InitMetric(default_options);
  }

  int idx = 0;
  while (true) {
    bool wrote = false;

    for (auto& [hash, series] : time_series) {
      if (idx >= series.size()) {
        continue;
      }
      auto& cur_time_series = series[idx];
      storage.Write(metric_ids[hash], cur_time_series);
      wrote = true;
    }

    ++idx;

    if (!wrote) {
      break;
    }
  }

  end = std::chrono::steady_clock::now();
  std::cerr << "write time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << "ms" << std::endl;
  std::vector<tskv::MetricId> metric_ids_vec;
  for (const auto& [_, metric_id] : metric_ids) {
    metric_ids_vec.push_back(metric_id);
  }
  return {{*min, *max}, metric_ids_vec};
}

struct Query {
  tskv::MetricId metric_id;
  tskv::TimeRange time_range;
  tskv::AggregationType aggregation_type;
};

void Read(tskv::Storage& storage, const tskv::TimeRange& time_range,
          const std::vector<tskv::MetricId>& metric_ids) {
  constexpr int kQueries = 1e5;
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> start_dis(time_range.start,
                                                    time_range.end);
  std::uniform_int_distribution<size_t> metric_id_dis(0, 4);
  std::vector<Query> queries;
  for (int i = 0; i < kQueries; ++i) {
    auto metric_id = metric_ids[metric_id_dis(gen)];
    auto start = start_dis(gen);
    auto end = start_dis(gen);
    if (start > end) {
      std::swap(start, end);
    }
    auto aggregation_type = static_cast<tskv::AggregationType>(
        std::uniform_int_distribution<>(1, 4)(gen));
    queries.push_back({metric_id, {start, end}, aggregation_type});
  }

  std::cerr << "starting read" << std::endl;

  std::vector<tskv::Column> result(kQueries);
  size_t idx = 0;
  auto start = std::chrono::steady_clock::now();
  for (const auto& query : queries) {
    result[idx++] =
        storage.Read(query.metric_id, query.time_range, query.aggregation_type);
  }
  auto end = std::chrono::steady_clock::now();
  auto ms_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  std::cerr << "query time: " << ms_time << "ms" << std::endl;
}

int main() {
  tskv::Storage storage;
  auto [time_range, metric_ids] = Write(storage);
  Read(storage, time_range, metric_ids);
  return 0;
}
