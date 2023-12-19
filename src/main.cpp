#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "model/column.h"
#include "model/model.h"
#include "persistent-storage/disk_storage.h"
#include "storage/storage.h"

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
  int64_t write_time;
};

WriteResult Write(tskv::Storage& storage) {
  auto start = std::chrono::steady_clock::now();
  std::ifstream input("../../test_data/timescaledb-data-8-1s-24h");
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

  tskv::MetricStorage::Options default_options = {
      tskv::MetricOptions{
          {
              tskv::StoredAggregationType::kSum,
              tskv::StoredAggregationType::kCount,
              tskv::StoredAggregationType::kMin,
              tskv::StoredAggregationType::kMax,
              tskv::StoredAggregationType::kLast,
          },
      },
      tskv::Memtable::Options{
          .bucket_interval = tskv::Duration::Seconds(10),
          .max_bytes_size = 100 * kMb,
          .max_age = tskv::Duration::Hours(5),
          .store_raw = true,
      },
      tskv::PersistentStorageManager::Options{
          .levels = {{
                         .bucket_interval = tskv::Duration::Seconds(10),
                         .level_duration = tskv::Duration::Hours(10),
                         .store_raw = true,
                     },
                     {
                         .bucket_interval = tskv::Duration::Seconds(30),
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

  storage.Flush();

  auto end = std::chrono::steady_clock::now();
  auto ms_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  std::vector<tskv::MetricId> metric_ids_vec;
  for (const auto& [_, metric_id] : metric_ids) {
    metric_ids_vec.push_back(metric_id);
  }
  return {{*min, *max}, metric_ids_vec, ms_time};
}

struct Query {
  std::vector<tskv::MetricId> metric_ids;
  tskv::TimeRange time_range;
  tskv::AggregationType aggregation_type;
};

struct SingleGroupByParams {
  size_t metric_count;
  size_t host_count;
  tskv::Duration aggregation_window;
  tskv::Duration query_range;

  friend std::ostream& operator<<(std::ostream& os,
                                  const SingleGroupByParams& params) {
    return os << "metric_count: " << params.metric_count
              << ", aggregation_window: " << params.aggregation_window
              << ", query_range: " << params.query_range;
  }
};

tskv::Column MergeColumns(const std::vector<tskv::Column>& columns) {
  if (columns.empty()) {
    return {};
  }
  auto result = columns[0];
  for (size_t i = 1; i < columns.size(); ++i) {
    result->Merge(columns[i]);
  }
  return result;
}

// returns requsts per second
double SingleGroupBy(tskv::Storage& storage, const tskv::TimeRange& time_range,
                     const std::vector<tskv::MetricId>& metric_ids,
                     const SingleGroupByParams& params) {
  constexpr uint64_t kQueries = 1e4;
  constexpr int kSeed = 123;
  std::mt19937_64 gen(kSeed);
  auto start_dis_end = time_range.end - params.query_range;
  std::uniform_int_distribution<uint64_t> start_dis(time_range.start,
                                                    start_dis_end);
  std::uniform_int_distribution<size_t> metric_id_dis(0, metric_ids.size() - 1);
  std::vector<Query> queries;
  for (int i = 0; i < kQueries; ++i) {
    std::vector<tskv::MetricId> metric_ids_vec;
    for (size_t j = 0; j < params.metric_count * params.host_count; ++j) {
      metric_ids_vec.push_back(metric_ids[metric_id_dis(gen)]);
    }
    auto start = start_dis(gen);
    auto end = start + params.query_range;
    queries.push_back(
        {metric_ids_vec, {start, end}, tskv::AggregationType::kMax});
  }

  tskv::Column temp_result;
  std::vector<tskv::Column> buffer;
  auto start = std::chrono::steady_clock::now();
  for (const auto& query : queries) {

    for (const auto& metric_id : query.metric_ids) {
      auto result =
          storage.Read(metric_id, query.time_range, query.aggregation_type);
      auto max_column = std::dynamic_pointer_cast<tskv::MaxColumn>(result);
      max_column->ScaleBuckets(params.aggregation_window);
      buffer.push_back(std::move(result));
    }

    for (int i = 0; i < params.metric_count; ++i) {
      std::vector<tskv::Column> to_merge;
      for (int j = 0; j < params.host_count; ++j) {
        to_merge.push_back(std::move(buffer.back()));
        buffer.pop_back();
      }
      temp_result = MergeColumns(to_merge);
    }
  }
  auto end = std::chrono::steady_clock::now();
  auto ms_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  (void)temp_result;
  return kQueries * 1000.0 / ms_time;
}

int main() {
  tskv::Storage storage;
  auto [time_range, metric_ids, write_time] = Write(storage);
  std::cout << "write time: " << write_time << "ms" << std::endl;
  auto five_minutes = tskv::Duration::Minutes(5);
  auto one_hour = tskv::Duration::Hours(1);
  auto twelve_hours = tskv::Duration::Hours(12);
  auto params = std::vector<SingleGroupByParams>{
      {1, 1, five_minutes, one_hour},     {1, 1, five_minutes, twelve_hours},
      {1, 8, five_minutes, one_hour},     {5, 1, five_minutes, one_hour},
      {5, 1, five_minutes, twelve_hours}, {5, 8, five_minutes, one_hour},
  };

  std::vector<double> read_rps;
  read_rps.reserve(params.size());
for (const auto& param : params) {
    read_rps.push_back(SingleGroupBy(storage, time_range, metric_ids, param));
  }

  std::ofstream output("performance.txt");
  output << "write time: " << write_time << "ms" << std::endl;
  for (size_t i = 0; i < params.size(); ++i) {
    output << params[i] << " read rps: " << read_rps[i] << std::endl;
  }
  output.close();

  std::filesystem::remove_all("./tmp/tskv");
}
