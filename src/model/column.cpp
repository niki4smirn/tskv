#include "column.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <cwchar>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace tskv {

AggregateColumn::AggregateColumn(Duration bucket_interval)
    : bucket_interval_(bucket_interval) {}

AggregateColumn::AggregateColumn(std::vector<double> buckets,
                                 const TimePoint& start_time,
                                 Duration bucket_interval)
    : buckets_(std::move(buckets)),
      start_time_(start_time),
      bucket_interval_(bucket_interval) {
  auto time_range = GetTimeRange();
  assert(buckets_.size() ==
         (time_range.end - time_range.start + bucket_interval_ - 1) /
             bucket_interval_);
  assert(start_time % bucket_interval_ == 0);
}

ReadColumn AggregateColumn::Read(const TimeRange& time_range,
                                 ColumnType column_type) const {
  if (buckets_.empty()) {
    return std::shared_ptr<SumColumn>(nullptr);
  }
  auto start_bucket = *GetBucketIdx(time_range.start);
  auto end_bucket = *GetBucketIdx(time_range.end);
  if (end_bucket < buckets_.size() && time_range.end % bucket_interval_ != 0) {
    ++end_bucket;
  }
  if (start_bucket == end_bucket) {
    return std::shared_ptr<SumColumn>(nullptr);
  }
  auto new_start_time = start_time_;
  if (time_range.start > start_time_) {
    new_start_time =
        time_range.start - (time_range.start - start_time_) % bucket_interval_;
  }
  auto data = std::vector<double>(buckets_.begin() + start_bucket,
                                  buckets_.begin() + end_bucket);
  switch (column_type) {
    case ColumnType::kSum: {
      return std::make_shared<SumColumn>(std::move(data), new_start_time,
                                         bucket_interval_);
    }
    case ColumnType::kCount: {
      return std::make_shared<CountColumn>(std::move(data), new_start_time,
                                           bucket_interval_);
    }
    case ColumnType::kMin: {
      return std::make_shared<MinColumn>(std::move(data), new_start_time,
                                         bucket_interval_);
    }
    case ColumnType::kMax: {
      return std::make_shared<MaxColumn>(std::move(data), new_start_time,
                                         bucket_interval_);
    }
    case ColumnType::kLast: {
      return std::make_shared<LastColumn>(std::move(data), new_start_time,
                                          bucket_interval_);
    }
    case ColumnType::kAvg: {
      return std::make_shared<AvgColumn>(std::move(data), new_start_time,
                                         bucket_interval_);
    }
    default:
      throw std::runtime_error("Unknown column type");
  }
}

CompressedBytes AggregateColumn::ToBytes() const {
  CompressedBytes res;
  Append(res, bucket_interval_);
  Append(res, start_time_);
  Append(res, buckets_.data(), buckets_.size());
  return res;
}

std::optional<size_t> AggregateColumn::GetBucketIdx(TimePoint timestamp) const {
  if (timestamp < start_time_) {
    return 0;
  }

  auto time_range = GetTimeRange();
  if (timestamp >= time_range.end) {
    return buckets_.size();
  }

  return (timestamp - start_time_) / bucket_interval_;
}

std::vector<Value> AggregateColumn::GetValues() const {
  return buckets_;
}

TimeRange AggregateColumn::GetTimeRange() const {
  return {start_time_, start_time_ + buckets_.size() * bucket_interval_};
}

Column AggregateColumn::Extract(ColumnType column_type) {
  std::shared_ptr<IAggregateColumn> col;
  switch (column_type) {
    case ColumnType::kSum: {
      col = std::make_shared<SumColumn>(std::move(buckets_), start_time_,
                                        bucket_interval_);
      break;
    }
    case ColumnType::kCount: {
      col = std::make_shared<CountColumn>(std::move(buckets_), start_time_,
                                          bucket_interval_);
      break;
    }
    case ColumnType::kMin: {
      col = std::make_shared<MinColumn>(std::move(buckets_), start_time_,
                                        bucket_interval_);
      break;
    }
    case ColumnType::kMax: {
      col = std::make_shared<MaxColumn>(std::move(buckets_), start_time_,
                                        bucket_interval_);
      break;
    }
    case ColumnType::kLast: {
      col = std::make_shared<LastColumn>(std::move(buckets_), start_time_,
                                         bucket_interval_);
      break;
    }
    default:
      throw std::runtime_error("Type " +
                               std::to_string(static_cast<int>(column_type)) +
                               " is not to extract");
  }
  buckets_ = {};
  start_time_ = 0;
  auto read_column = std::static_pointer_cast<IReadColumn>(col);
  return std::static_pointer_cast<IColumn>(read_column);
}

SumColumn::SumColumn(Duration bucket_interval)
    : column_(bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

SumColumn::SumColumn(std::vector<double> buckets, const TimePoint& start_time,
                     Duration bucket_interval)
    : column_(std::move(buckets), start_time, bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

ColumnType SumColumn::GetType() const {
  return ColumnType::kSum;
}

void SumColumn::ScaleBuckets(Duration bucket_interval) {
  if (bucket_interval == bucket_interval_) {
    return;
  }
  assert(bucket_interval % bucket_interval_ == 0);
  auto scale = bucket_interval / bucket_interval_;
  auto new_buckets_sz = buckets_.size() / scale;
  if (start_time_ % bucket_interval != 0 || buckets_.size() % scale != 0) {
    ++new_buckets_sz;
  }

  double sum = 0;
  bool updated = false;
  size_t pos = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    sum += buckets_[i];
    updated = true;
    if ((start_time_ + bucket_interval_ * i) / bucket_interval !=
        (start_time_ + bucket_interval_ * (i + 1)) / bucket_interval) {
      buckets_[pos++] = sum;
      sum = 0;
      updated = false;
    }
  }

  if (updated) {
    buckets_[pos++] = sum;
  }

  assert(pos == new_buckets_sz);

  start_time_ = start_time_ - start_time_ % bucket_interval;
  bucket_interval_ = bucket_interval;
  buckets_.resize(new_buckets_sz);
}

void SumColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto sum_column = std::dynamic_pointer_cast<SumColumn>(column);
  if (!sum_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == sum_column.get()) {
    return;
  }
  if (sum_column->bucket_interval_ != bucket_interval_) {
    if (sum_column->bucket_interval_ < bucket_interval_) {
      sum_column->ScaleBuckets(bucket_interval_);
    } else {
      ScaleBuckets(sum_column->bucket_interval_);
    }
  }
  if (buckets_.empty()) {
    buckets_ = sum_column->buckets_;
    start_time_ = sum_column->start_time_;
    return;
  }
  if (sum_column->buckets_.empty()) {
    return;
  }
  if (sum_column->start_time_ < start_time_) {
    throw std::runtime_error("Wrong merge order");
  }

  auto sum_column_time_range = sum_column->GetTimeRange();
  auto intersection_start_opt =
      column_.GetBucketIdx(sum_column_time_range.start);
  auto intersection_end_opt = column_.GetBucketIdx(sum_column_time_range.end);
  auto intersection_end =
      intersection_end_opt ? *intersection_end_opt : buckets_.size();
  auto intersection_start =
      intersection_end_opt ? *intersection_start_opt : buckets_.size();
  for (size_t i = intersection_start; i < intersection_end; ++i) {
    buckets_[i] += sum_column->buckets_[i - intersection_start];
  }

  auto cur_time_range = GetTimeRange();
  if (sum_column->start_time_ > cur_time_range.end) {
    auto to_insert_zeroes =
        (sum_column->start_time_ - cur_time_range.end) / bucket_interval_;
    for (size_t i = 0; i < to_insert_zeroes; ++i) {
      buckets_.push_back(0);
    }
  }

  auto to_skip = intersection_start ? intersection_end - intersection_start : 0;
  for (const auto& val : std::views::drop(sum_column->buckets_, to_skip)) {
    buckets_.push_back(val);
  }
}

void SumColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  if (buckets_.empty()) {
    start_time_ = time_series.front().timestamp -
                  time_series.front().timestamp % bucket_interval_;
  }
  assert(start_time_ % bucket_interval_ == 0);
  assert(time_series.front().timestamp >= start_time_);
  auto needed_size =
      (time_series.back().timestamp + 1 - start_time_ + bucket_interval_ - 1) /
      bucket_interval_;
  buckets_.resize(needed_size);
  for (const auto& record : time_series) {
    auto idx = *column_.GetBucketIdx(record.timestamp);
    buckets_[idx] += record.value;
  }
}

ReadColumn SumColumn::Read(const TimeRange& time_range) const {
  return column_.Read(time_range, ColumnType::kSum);
}

std::vector<Value> SumColumn::GetValues() const {
  return column_.GetValues();
}

TimeRange SumColumn::GetTimeRange() const {
  return column_.GetTimeRange();
}

Column SumColumn::Extract() {
  return column_.Extract(ColumnType::kSum);
}

CompressedBytes SumColumn::ToBytes() const {
  return column_.ToBytes();
}

size_t SumColumn::GetBucketsNum() const {
  return buckets_.size();
}

CountColumn::CountColumn(Duration bucket_interval)
    : column_(bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

CountColumn::CountColumn(std::vector<double> buckets,
                         const TimePoint& start_time, Duration bucket_interval)
    : column_(std::move(buckets), start_time, bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

ColumnType CountColumn::GetType() const {
  return ColumnType::kCount;
}

void CountColumn::ScaleBuckets(Duration bucket_interval) {
  if (bucket_interval == bucket_interval_) {
    return;
  }
  assert(bucket_interval % bucket_interval_ == 0);
  auto scale = bucket_interval / bucket_interval_;
  auto new_buckets_sz = buckets_.size() / scale;
  if (start_time_ % bucket_interval != 0 || buckets_.size() % scale != 0) {
    ++new_buckets_sz;
  }

  size_t count = 0;
  bool updated = false;
  size_t pos = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    count += buckets_[i];
    updated = true;
    if ((start_time_ + bucket_interval_ * i) / bucket_interval !=
        (start_time_ + bucket_interval_ * (i + 1)) / bucket_interval) {
      buckets_[pos++] = count;
      count = 0;
      updated = false;
    }
  }

  if (updated) {
    buckets_[pos++] = count;
  }

  assert(pos == new_buckets_sz);

  start_time_ = start_time_ - start_time_ % bucket_interval;
  bucket_interval_ = bucket_interval;
  buckets_.resize(new_buckets_sz);
}

void CountColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto count_column = std::dynamic_pointer_cast<CountColumn>(column);
  if (!count_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == count_column.get()) {
    return;
  }
  if (count_column->bucket_interval_ != bucket_interval_) {
    if (count_column->bucket_interval_ < bucket_interval_) {
      count_column->ScaleBuckets(bucket_interval_);
    } else {
      ScaleBuckets(count_column->bucket_interval_);
    }
  }
  if (buckets_.empty()) {
    buckets_ = count_column->buckets_;
    start_time_ = count_column->start_time_;
    return;
  }
  if (count_column->buckets_.empty()) {
    return;
  }
  if (count_column->start_time_ < start_time_) {
    throw std::runtime_error("Wrong merge order");
  }

  auto count_column_time_range = count_column->GetTimeRange();
  auto intersection_start_opt =
      column_.GetBucketIdx(count_column_time_range.start);
  auto intersection_end_opt = column_.GetBucketIdx(count_column_time_range.end);
  auto intersection_end =
      intersection_end_opt ? *intersection_end_opt : buckets_.size();
  auto intersection_start =
      intersection_end_opt ? *intersection_start_opt : buckets_.size();
  for (size_t i = intersection_start; i < intersection_end; ++i) {
    buckets_[i] += count_column->buckets_[i - intersection_start];
  }

  auto cur_time_range = GetTimeRange();
  if (count_column->start_time_ > cur_time_range.end) {
    auto to_insert_zeroes =
        (count_column->start_time_ - cur_time_range.end) / bucket_interval_;
    for (size_t i = 0; i < to_insert_zeroes; ++i) {
      buckets_.push_back(0);
    }
  }

  auto to_skip = intersection_start ? intersection_end - intersection_start : 0;
  for (const auto& val : std::views::drop(count_column->buckets_, to_skip)) {
    buckets_.push_back(val);
  }
}

void CountColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  if (buckets_.empty()) {
    start_time_ = time_series.front().timestamp -
                  time_series.front().timestamp % bucket_interval_;
  }
  assert(start_time_ % bucket_interval_ == 0);
  assert(time_series.front().timestamp >= start_time_);
  auto needed_size =
      (time_series.back().timestamp + 1 - start_time_ + bucket_interval_ - 1) /
      bucket_interval_;
  buckets_.resize(needed_size);
  for (const auto& record : time_series) {
    auto idx = *column_.GetBucketIdx(record.timestamp);
    ++buckets_[idx];
  }
}

ReadColumn CountColumn::Read(const TimeRange& time_range) const {
  return column_.Read(time_range, ColumnType::kCount);
}

std::vector<Value> CountColumn::GetValues() const {
  return column_.GetValues();
}

TimeRange CountColumn::GetTimeRange() const {
  return column_.GetTimeRange();
}

Column CountColumn::Extract() {
  return column_.Extract(ColumnType::kCount);
}

CompressedBytes CountColumn::ToBytes() const {
  return column_.ToBytes();
}

size_t CountColumn::GetBucketsNum() const {
  return buckets_.size();
}

MinColumn::MinColumn(Duration bucket_interval)
    : column_(bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

MinColumn::MinColumn(std::vector<double> buckets, const TimePoint& start_time,
                     Duration bucket_interval)
    : column_(std::move(buckets), start_time, bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

ColumnType MinColumn::GetType() const {
  return ColumnType::kMin;
}

void MinColumn::ScaleBuckets(Duration bucket_interval) {
  if (bucket_interval == bucket_interval_) {
    return;
  }
  assert(bucket_interval % bucket_interval_ == 0);
  auto scale = bucket_interval / bucket_interval_;
  auto new_buckets_sz = buckets_.size() / scale;
  if (start_time_ % bucket_interval != 0 || buckets_.size() % scale != 0) {
    ++new_buckets_sz;
  }

  double min = std::numeric_limits<double>::max();
  bool updated = false;
  size_t pos = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    min = std::min(min, buckets_[i]);
    updated = true;
    if ((start_time_ + bucket_interval_ * i) / bucket_interval !=
        (start_time_ + bucket_interval_ * (i + 1)) / bucket_interval) {
      buckets_[pos++] = min;
      min = std::numeric_limits<double>::max();
      updated = false;
    }
  }

  if (updated) {
    buckets_[pos++] = min;
  }

  assert(pos == new_buckets_sz);

  start_time_ = start_time_ - start_time_ % bucket_interval;
  bucket_interval_ = bucket_interval;
  buckets_.resize(new_buckets_sz);
}

void MinColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto min_column = std::dynamic_pointer_cast<MinColumn>(column);
  if (!min_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == min_column.get()) {
    return;
  }
  if (min_column->bucket_interval_ != bucket_interval_) {
    if (min_column->bucket_interval_ < bucket_interval_) {
      min_column->ScaleBuckets(bucket_interval_);
    } else {
      ScaleBuckets(min_column->bucket_interval_);
    }
  }
  if (buckets_.empty()) {
    buckets_ = min_column->buckets_;
    start_time_ = min_column->start_time_;
    return;
  }
  if (min_column->buckets_.empty()) {
    return;
  }
  if (min_column->start_time_ < start_time_) {
    throw std::runtime_error("Wrong merge order");
  }

  auto min_column_time_range = min_column->GetTimeRange();
  auto intersection_start_opt =
      column_.GetBucketIdx(min_column_time_range.start);
  auto intersection_end_opt = column_.GetBucketIdx(min_column_time_range.end);
  auto intersection_end =
      intersection_end_opt ? *intersection_end_opt : buckets_.size();
  auto intersection_start =
      intersection_end_opt ? *intersection_start_opt : buckets_.size();
  for (size_t i = intersection_start; i < intersection_end; ++i) {
    buckets_[i] =
        std::min(buckets_[i], min_column->buckets_[i - intersection_start]);
  }

  auto cur_time_range = GetTimeRange();
  if (min_column->start_time_ > cur_time_range.end) {
    auto to_insert_max =
        (min_column->start_time_ - cur_time_range.end) / bucket_interval_;
    for (size_t i = 0; i < to_insert_max; ++i) {
      buckets_.push_back(std::numeric_limits<double>::max());
    }
  }

  auto to_skip = intersection_start ? intersection_end - intersection_start : 0;
  for (const auto& val : std::views::drop(min_column->buckets_, to_skip)) {
    buckets_.push_back(val);
  }
}

void MinColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  if (buckets_.empty()) {
    start_time_ = time_series.front().timestamp -
                  time_series.front().timestamp % bucket_interval_;
  }
  assert(start_time_ % bucket_interval_ == 0);
  assert(time_series.front().timestamp >= start_time_);
  auto needed_size =
      (time_series.back().timestamp + 1 - start_time_ + bucket_interval_ - 1) /
      bucket_interval_;
  buckets_.resize(needed_size, std::numeric_limits<double>::max());
  for (const auto& record : time_series) {
    auto idx = *column_.GetBucketIdx(record.timestamp);
    buckets_[idx] = std::min(buckets_[idx], record.value);
  }
}

ReadColumn MinColumn::Read(const TimeRange& time_range) const {
  return column_.Read(time_range, ColumnType::kMin);
}

std::vector<Value> MinColumn::GetValues() const {
  return column_.GetValues();
}

TimeRange MinColumn::GetTimeRange() const {
  return column_.GetTimeRange();
}

Column MinColumn::Extract() {
  return column_.Extract(ColumnType::kMin);
}

CompressedBytes MinColumn::ToBytes() const {
  return column_.ToBytes();
}

size_t MinColumn::GetBucketsNum() const {
  return buckets_.size();
}

MaxColumn::MaxColumn(Duration bucket_interval)
    : column_(bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

MaxColumn::MaxColumn(std::vector<double> buckets, const TimePoint& start_time,
                     Duration bucket_interval)
    : column_(std::move(buckets), start_time, bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

ColumnType MaxColumn::GetType() const {
  return ColumnType::kMax;
}

void MaxColumn::ScaleBuckets(Duration bucket_interval) {
  if (bucket_interval == bucket_interval_) {
    return;
  }
  assert(bucket_interval % bucket_interval_ == 0);
  auto scale = bucket_interval / bucket_interval_;
  auto new_buckets_sz = buckets_.size() / scale;
  if (start_time_ % bucket_interval != 0 || buckets_.size() % scale != 0) {
    ++new_buckets_sz;
  }

  double max = std::numeric_limits<double>::lowest();
  bool updated = false;
  size_t pos = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    max = std::max(max, buckets_[i]);
    updated = true;
    if ((start_time_ + bucket_interval_ * i) / bucket_interval !=
        (start_time_ + bucket_interval_ * (i + 1)) / bucket_interval) {
      buckets_[pos++] = max;
      max = std::numeric_limits<double>::lowest();
      updated = false;
    }
  }

  if (updated) {
    buckets_[pos++] = max;
  }

  assert(pos == new_buckets_sz);

  start_time_ = start_time_ - start_time_ % bucket_interval;
  bucket_interval_ = bucket_interval;
  buckets_.resize(new_buckets_sz);
}

void MaxColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto max_column = std::dynamic_pointer_cast<MaxColumn>(column);
  if (!max_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == max_column.get()) {
    return;
  }
  if (max_column->bucket_interval_ != bucket_interval_) {
    if (max_column->bucket_interval_ < bucket_interval_) {
      max_column->ScaleBuckets(bucket_interval_);
    } else {
      ScaleBuckets(max_column->bucket_interval_);
    }
  }
  if (buckets_.empty()) {
    buckets_ = max_column->buckets_;
    start_time_ = max_column->start_time_;
    return;
  }
  if (max_column->buckets_.empty()) {
    return;
  }
  if (max_column->start_time_ < start_time_) {
    throw std::runtime_error("Wrong merge order");
  }

  auto max_column_time_range = max_column->GetTimeRange();
  auto intersection_start_opt =
      column_.GetBucketIdx(max_column_time_range.start);
  auto intersection_end_opt = column_.GetBucketIdx(max_column_time_range.end);
  auto intersection_end =
      intersection_end_opt ? *intersection_end_opt : buckets_.size();
  auto intersection_start =
      intersection_end_opt ? *intersection_start_opt : buckets_.size();
  for (size_t i = intersection_start; i < intersection_end; ++i) {
    buckets_[i] =
        std::max(buckets_[i], max_column->buckets_[i - intersection_start]);
  }

  auto cur_time_range = GetTimeRange();
  if (max_column->start_time_ > cur_time_range.end) {
    auto to_insert_min =
        (max_column->start_time_ - cur_time_range.end) / bucket_interval_;
    for (size_t i = 0; i < to_insert_min; ++i) {
      buckets_.push_back(std::numeric_limits<double>::lowest());
    }
  }

  auto to_skip = intersection_start ? intersection_end - intersection_start : 0;
  for (const auto& val : std::views::drop(max_column->buckets_, to_skip)) {
    buckets_.push_back(val);
  }
}

void MaxColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  if (buckets_.empty()) {
    start_time_ = time_series.front().timestamp -
                  time_series.front().timestamp % bucket_interval_;
  }
  assert(start_time_ % bucket_interval_ == 0);
  assert(time_series.front().timestamp >= start_time_);
  auto needed_size =
      (time_series.back().timestamp + 1 - start_time_ + bucket_interval_ - 1) /
      bucket_interval_;
  buckets_.resize(needed_size, std::numeric_limits<double>::lowest());
  for (const auto& record : time_series) {
    auto idx = *column_.GetBucketIdx(record.timestamp);
    buckets_[idx] = std::max(buckets_[idx], record.value);
  }
}

ReadColumn MaxColumn::Read(const TimeRange& time_range) const {
  return column_.Read(time_range, ColumnType::kMax);
}

std::vector<Value> MaxColumn::GetValues() const {
  return column_.GetValues();
}

TimeRange MaxColumn::GetTimeRange() const {
  return column_.GetTimeRange();
}

Column MaxColumn::Extract() {
  return column_.Extract(ColumnType::kMax);
}

CompressedBytes MaxColumn::ToBytes() const {
  return column_.ToBytes();
}

size_t MaxColumn::GetBucketsNum() const {
  return buckets_.size();
}

LastColumn::LastColumn(Duration bucket_interval)
    : column_(bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

LastColumn::LastColumn(std::vector<double> buckets, const TimePoint& start_time,
                       Duration bucket_interval)
    : column_(std::move(buckets), start_time, bucket_interval),
      buckets_(column_.buckets_),
      start_time_(column_.start_time_),
      bucket_interval_(column_.bucket_interval_) {}

ColumnType LastColumn::GetType() const {
  return ColumnType::kLast;
}

void LastColumn::ScaleBuckets(Duration bucket_interval) {
  if (bucket_interval == bucket_interval_) {
    return;
  }
  assert(bucket_interval % bucket_interval_ == 0);
  auto scale = bucket_interval / bucket_interval_;
  auto new_buckets_sz = buckets_.size() / scale;
  if (start_time_ % bucket_interval != 0 || buckets_.size() % scale != 0) {
    ++new_buckets_sz;
  }

  double last = 0;
  bool updated = false;
  size_t pos = 0;
  for (size_t i = 0; i < buckets_.size(); ++i) {
    last = buckets_[i];
    updated = true;
    if ((start_time_ + bucket_interval_ * i) / bucket_interval !=
        (start_time_ + bucket_interval_ * (i + 1)) / bucket_interval) {
      buckets_[pos++] = last;
      last = 0;
      updated = false;
    }
  }

  if (updated) {
    buckets_[pos++] = last;
  }

  assert(pos == new_buckets_sz);

  start_time_ = start_time_ - start_time_ % bucket_interval;
  bucket_interval_ = bucket_interval;
  buckets_.resize(new_buckets_sz);
}

void LastColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto last_column = std::dynamic_pointer_cast<LastColumn>(column);
  if (!last_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == last_column.get()) {
    return;
  }
  if (last_column->bucket_interval_ != bucket_interval_) {
    if (last_column->bucket_interval_ < bucket_interval_) {
      last_column->ScaleBuckets(bucket_interval_);
    } else {
      ScaleBuckets(last_column->bucket_interval_);
    }
  }
  if (buckets_.empty()) {
    buckets_ = last_column->buckets_;
    start_time_ = last_column->start_time_;
    return;
  }
  if (last_column->buckets_.empty()) {
    return;
  }
  if (last_column->start_time_ < start_time_) {
    throw std::runtime_error("Wrong merge order");
  }

  auto last_column_time_range = last_column->GetTimeRange();
  auto intersection_start_opt =
      column_.GetBucketIdx(last_column_time_range.start);
  auto intersection_end_opt = column_.GetBucketIdx(last_column_time_range.end);
  auto intersection_end =
      intersection_end_opt ? *intersection_end_opt : buckets_.size();
  auto intersection_start =
      intersection_end_opt ? *intersection_start_opt : buckets_.size();
  for (size_t i = intersection_start; i < intersection_end; ++i) {
    buckets_[i] = last_column->buckets_[i - intersection_start];
  }

  auto cur_time_range = GetTimeRange();
  if (last_column->start_time_ > cur_time_range.end) {
    auto to_insert_zeroes =
        (last_column->start_time_ - cur_time_range.end) / bucket_interval_;
    for (size_t i = 0; i < to_insert_zeroes; ++i) {
      buckets_.push_back(0);
    }
  }

  auto to_skip = intersection_start ? intersection_end - intersection_start : 0;
  for (const auto& val : std::views::drop(last_column->buckets_, to_skip)) {
    buckets_.push_back(val);
  }
}

void LastColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  if (buckets_.empty()) {
    start_time_ = time_series.front().timestamp -
                  time_series.front().timestamp % bucket_interval_;
  }
  assert(start_time_ % bucket_interval_ == 0);
  assert(time_series.front().timestamp >= start_time_);
  auto needed_size =
      (time_series.back().timestamp + 1 - start_time_ + bucket_interval_ - 1) /
      bucket_interval_;
  buckets_.resize(needed_size);
  for (const auto& record : time_series) {
    auto idx = *column_.GetBucketIdx(record.timestamp);
    buckets_[idx] = record.value;
  }
}

ReadColumn LastColumn::Read(const TimeRange& time_range) const {
  return column_.Read(time_range, ColumnType::kLast);
}

std::vector<Value> LastColumn::GetValues() const {
  return column_.GetValues();
}

TimeRange LastColumn::GetTimeRange() const {
  return column_.GetTimeRange();
}

Column LastColumn::Extract() {
  return column_.Extract(ColumnType::kLast);
}

CompressedBytes LastColumn::ToBytes() const {
  return column_.ToBytes();
}

size_t LastColumn::GetBucketsNum() const {
  return buckets_.size();
}

RawTimestampsColumn::RawTimestampsColumn(std::vector<TimePoint> timestamps)
    : timestamps_(std::move(timestamps)) {}

ColumnType RawTimestampsColumn::GetType() const {
  return ColumnType::kRawTimestamps;
}

CompressedBytes RawTimestampsColumn::ToBytes() const {
  CompressedBytes res;
  Append(res, timestamps_.data(), timestamps_.size());
  return res;
}

void RawTimestampsColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto raw_timestamps_column =
      std::dynamic_pointer_cast<RawTimestampsColumn>(column);
  if (!raw_timestamps_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == raw_timestamps_column.get()) {
    return;
  }
  if (timestamps_.empty()) {
    timestamps_ = raw_timestamps_column->timestamps_;
    return;
  }
  if (raw_timestamps_column->timestamps_.empty()) {
    return;
  }
  if (raw_timestamps_column->timestamps_.front() < timestamps_.back()) {
    throw std::runtime_error("Wrong merge order");
  }
  timestamps_.insert(timestamps_.end(),
                     raw_timestamps_column->timestamps_.begin(),
                     raw_timestamps_column->timestamps_.end());
}

void RawTimestampsColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  timestamps_.reserve(timestamps_.size() + time_series.size());
  for (const auto& record : time_series) {
    timestamps_.push_back(record.timestamp);
  }
}

std::vector<Value> RawTimestampsColumn::GetValues() const {
  return {timestamps_.begin(), timestamps_.end()};
}

Column RawTimestampsColumn::Extract() {
  auto timestamps = std::move(timestamps_);
  timestamps_ = {};
  return std::make_shared<RawTimestampsColumn>(timestamps);
}

TimeRange RawTimestampsColumn::GetTimeRange() const {
  if (timestamps_.empty()) {
    return {};
  }
  return {timestamps_.front(), timestamps_.back() + 1};
}

size_t RawTimestampsColumn::TimestampsNum() const {
  return timestamps_.size();
}

RawValuesColumn::RawValuesColumn(std::vector<Value> values)
    : values_(std::move(values)) {}

ColumnType RawValuesColumn::GetType() const {
  return ColumnType::kRawValues;
}

CompressedBytes RawValuesColumn::ToBytes() const {
  CompressedBytes res;
  Append(res, values_.data(), values_.size());
  return res;
}

void RawValuesColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto raw_values_column = std::dynamic_pointer_cast<RawValuesColumn>(column);
  if (!raw_values_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == raw_values_column.get()) {
    return;
  }
  if (values_.empty()) {
    values_ = raw_values_column->values_;
    return;
  }
  if (raw_values_column->values_.empty()) {
    return;
  }
  values_.insert(values_.end(), raw_values_column->values_.begin(),
                 raw_values_column->values_.end());
}

void RawValuesColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  values_.reserve(values_.size() + time_series.size());
  for (const auto& record : time_series) {
    values_.push_back(record.value);
  }
}

std::vector<Value> RawValuesColumn::GetValues() const {
  return values_;
}

Column RawValuesColumn::Extract() {
  auto values = std::move(values_);
  values_ = {};
  return std::make_shared<RawValuesColumn>(values);
}

size_t RawValuesColumn::ValuesNum() const {
  return values_.size();
}

ReadRawColumn::ReadRawColumn(
    std::shared_ptr<RawTimestampsColumn> timestamps_column,
    std::shared_ptr<RawValuesColumn> values_column)
    : timestamps_column_(std::move(timestamps_column)),
      values_column_(std::move(values_column)) {}

ColumnType ReadRawColumn::GetType() const {
  return ColumnType::kRawRead;
}

void ReadRawColumn::Merge(Column column) {
  if (!column) {
    return;
  }
  auto read_raw_column = std::dynamic_pointer_cast<ReadRawColumn>(column);
  if (!read_raw_column) {
    throw std::runtime_error("Can't merge columns of different types");
  }
  if (this == read_raw_column.get()) {
    return;
  }
  timestamps_column_->Merge(read_raw_column->timestamps_column_);
  values_column_->Merge(read_raw_column->values_column_);
}

ReadColumn ReadRawColumn::Read(const TimeRange& time_range) const {
  if (!timestamps_column_ || !values_column_) {
    return std::shared_ptr<ReadRawColumn>(nullptr);
  }
  auto& timestamps = timestamps_column_->timestamps_;
  auto& values = values_column_->values_;
  auto start =
      std::lower_bound(timestamps.begin(), timestamps.end(), time_range.start);
  auto end = std::upper_bound(start, timestamps.end(), time_range.end - 1);
  if (start == timestamps.end()) {
    return std::shared_ptr<ReadRawColumn>(nullptr);
  }
  return std::make_shared<ReadRawColumn>(
      std::make_shared<RawTimestampsColumn>(std::vector<TimePoint>(start, end)),
      std::make_shared<RawValuesColumn>(
          std::vector<Value>(values.begin() + (start - timestamps.begin()),
                             values.begin() + (end - timestamps.begin()))));
}

void ReadRawColumn::Write(const InputTimeSeries& time_series) {
  if (!timestamps_column_) {
    timestamps_column_ = std::make_shared<RawTimestampsColumn>();
  }
  if (!values_column_) {
    values_column_ = std::make_shared<RawValuesColumn>();
  }
  timestamps_column_->Write(time_series);
  values_column_->Write(time_series);
}

std::vector<Value> ReadRawColumn::GetValues() const {
  if (!values_column_) {
    return {};
  }
  return values_column_->GetValues();
}

TimeRange ReadRawColumn::GetTimeRange() const {
  return TimeRange{timestamps_column_->timestamps_.front(),
                   timestamps_column_->timestamps_.back() + 1};
}

Column ReadRawColumn::Extract() {
  auto timestamps_column = std::static_pointer_cast<RawTimestampsColumn>(
      timestamps_column_->Extract());
  auto values_column =
      std::static_pointer_cast<RawValuesColumn>(values_column_->Extract());
  timestamps_column_.reset();
  values_column_.reset();
  return std::make_shared<ReadRawColumn>(timestamps_column, values_column);
}

std::vector<TimePoint> ReadRawColumn::GetTimestamps() const {
  if (!timestamps_column_) {
    return {};
  }
  auto timestamps = timestamps_column_->GetValues();
  return {timestamps.begin(), timestamps.end()};
}

AvgColumn::AvgColumn(std::vector<double> buckets, const TimePoint& start_time,
                     Duration bucket_interval)
    : column_(std::move(buckets), start_time, bucket_interval) {}

AggregateColumn AvgColumn::CreateAvgAggregateColumn(
    std::shared_ptr<SumColumn> sum_column,
    std::shared_ptr<CountColumn> count_column) {
  assert(sum_column && count_column);
  if (sum_column->bucket_interval_ != count_column->bucket_interval_) {
    throw std::runtime_error(
        "Can't get avg of columns with different bucket "
        "intervals");
  }
  if (sum_column->start_time_ != count_column->start_time_) {
    throw std::runtime_error(
        "Can't get avg of columns with different start "
        "times");
  }
  std::vector<double> buckets;
  for (size_t i = 0; i < sum_column->buckets_.size(); ++i) {
    if (count_column->buckets_[i] == 0) {
      buckets.push_back(0);
    } else {
      buckets.push_back(sum_column->buckets_[i] / count_column->buckets_[i]);
    }
  }
  return {std::move(buckets), sum_column->start_time_,
          sum_column->bucket_interval_};
}

AvgColumn::AvgColumn(std::shared_ptr<SumColumn> sum_column,
                     std::shared_ptr<CountColumn> count_column)
    : column_(CreateAvgAggregateColumn(std::move(sum_column),
                                       std::move(count_column))) {}

ColumnType AvgColumn::GetType() const {
  return ColumnType::kAvg;
}

void AvgColumn::Merge(Column column) {
  assert(false);
}

ReadColumn AvgColumn::Read(const TimeRange& time_range) const {
  return column_.Read(time_range, ColumnType::kAvg);
}

void AvgColumn::Write(const InputTimeSeries& time_series) {
  assert(false);
}

std::vector<Value> AvgColumn::GetValues() const {
  return column_.GetValues();
}

TimeRange AvgColumn::GetTimeRange() const {
  return column_.GetTimeRange();
}

Column AvgColumn::Extract() {
  return column_.Extract(ColumnType::kAvg);
}

Column CreateRawColumn(ColumnType column_type) {
  switch (column_type) {
    case ColumnType::kRawValues:
      return std::make_shared<RawValuesColumn>();
    case ColumnType::kRawTimestamps:
      return std::make_shared<RawTimestampsColumn>();
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

Column CreateAggregatedColumn(ColumnType column_type,
                              Duration bucket_interval) {
  switch (column_type) {
    case ColumnType::kSum: {
      return CreateAggregatedColumn<SumColumn>(bucket_interval);
    }
    case ColumnType::kCount: {
      return CreateAggregatedColumn<CountColumn>(bucket_interval);
    }
    case ColumnType::kMin: {
      return CreateAggregatedColumn<MinColumn>(bucket_interval);
    }
    case ColumnType::kMax: {
      return CreateAggregatedColumn<MaxColumn>(bucket_interval);
    }
    case ColumnType::kLast: {
      return CreateAggregatedColumn<LastColumn>(bucket_interval);
    }
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

Column FromBytes(const CompressedBytes& bytes, ColumnType column_type) {
  switch (column_type) {
    case ColumnType::kRawValues: {
      auto data = reinterpret_cast<const Value*>(bytes.data());
      auto sz = bytes.size() / sizeof(Value);
      return std::make_shared<RawValuesColumn>(
          std::vector<Value>(data, data + sz));
    }
    case ColumnType::kRawTimestamps: {
      auto data = reinterpret_cast<const TimePoint*>(bytes.data());
      auto sz = bytes.size() / sizeof(TimePoint);
      return std::make_shared<RawTimestampsColumn>(
          std::vector<TimePoint>(data, data + sz));
    }
    case ColumnType::kSum: {
      return AggregateFromBytes<SumColumn>(bytes);
    }
    case ColumnType::kCount: {
      return AggregateFromBytes<CountColumn>(bytes);
    }
    case ColumnType::kMin: {
      return AggregateFromBytes<MinColumn>(bytes);
    }
    case ColumnType::kMax: {
      return AggregateFromBytes<MaxColumn>(bytes);
    }
    case ColumnType::kLast: {
      return AggregateFromBytes<LastColumn>(bytes);
    }
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

CompressedBytesReader::CompressedBytesReader(const CompressedBytes& bytes)
    : bytes_(bytes) {}

}  // namespace tskv
