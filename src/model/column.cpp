#include "column.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ranges>
#include <utility>

namespace tskv {

SumColumn::SumColumn(Duration bucket_interval)
    : bucket_interval_(bucket_interval) {}

SumColumn::SumColumn(std::vector<double> buckets, const TimeRange& time_range,
                     Duration bucket_interval)
    : buckets_(std::move(buckets)),
      time_range_(time_range),
      bucket_interval_(bucket_interval) {}

ColumnType SumColumn::GetType() const {
  return ColumnType::kSum;
}

CompressedBytes SumColumn::ToBytes() const {
  CompressedBytes res;
  Append(res, bucket_interval_);
  Append(res, time_range_.start);
  Append(res, time_range_.end);
  Append(res, buckets_.data(), buckets_.size());
  return res;
}

// column interval should start further than this->time_range.start
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
    throw std::runtime_error(
        "Can't merge columns with different bucket "
        "intervals");
  }
  if (buckets_.empty()) {
    buckets_ = sum_column->buckets_;
    time_range_ = sum_column->time_range_;
    return;
  }
  if (sum_column->buckets_.empty()) {
    return;
  }
  if (sum_column->time_range_.start < time_range_.start) {
    throw std::runtime_error("Wrong merge order");
  }

  auto intersection_start_opt = GetBucketIdx(sum_column->time_range_.start);
  auto intersection_end_opt = GetBucketIdx(sum_column->time_range_.end);
  auto intersection_end =
      intersection_end_opt ? *intersection_end_opt : buckets_.size();
  auto intersection_start =
      intersection_end_opt ? *intersection_start_opt : buckets_.size();
  for (size_t i = intersection_start; i < intersection_end; ++i) {
    buckets_[i] += sum_column->buckets_[i - intersection_start];
  }

  auto to_insert_zeroes =
      (sum_column->time_range_.start - time_range_.end) / bucket_interval_;
  for (size_t i = 0; i < to_insert_zeroes; ++i) {
    buckets_.push_back(0);
  }

  auto to_skip = intersection_start ? intersection_end - intersection_start : 0;
  for (const auto& val : std::views::drop(sum_column->buckets_, to_skip)) {
    buckets_.push_back(val);
  }

  time_range_.end = std::max(time_range_.end, sum_column->time_range_.end);
}

ReadColumn SumColumn::Read(const TimeRange& time_range) const {
  if (buckets_.empty()) {
    return std::shared_ptr<SumColumn>(nullptr);
  }
  auto start_bucket = *GetBucketIdx(time_range.start);
  auto end_bucket = *GetBucketIdx(time_range.end);
  auto new_time_range = TimeRange{
      std::max(time_range.start, time_range_.start),
      std::min(time_range.end, time_range_.end),
  };
  return std::make_shared<SumColumn>(
      std::vector<double>(buckets_.begin() + start_bucket,
                          buckets_.begin() + end_bucket),
      new_time_range, bucket_interval_);
}

void SumColumn::Write(const InputTimeSeries& time_series) {
  assert(std::ranges::is_sorted(time_series, {}, &Record::timestamp));
  if (buckets_.empty()) {
    // extend time range to the end of the bucket
    auto end = time_series.back().timestamp +
               (bucket_interval_ -
                (time_series.back().timestamp - time_series.front().timestamp) %
                    bucket_interval_);
    time_range_ = {time_series.front().timestamp, end};
  }
  auto needed_size = (time_series.back().timestamp + 1 - time_range_.start +
                      bucket_interval_ - 1) /
                     bucket_interval_;
  buckets_.resize(needed_size);
  for (const auto& record : time_series) {
    auto idx = *GetBucketIdx(record.timestamp);
    buckets_[idx] += record.value;
  }
}

std::optional<size_t> SumColumn::GetBucketIdx(TimePoint timestamp) const {
  if (timestamp < time_range_.start) {
    return 0;
  }

  if (timestamp >= time_range_.end) {
    return buckets_.size();
  }

  return (timestamp - time_range_.start) / bucket_interval_;
}

std::vector<Value> SumColumn::GetValues() const {
  return buckets_;
}

TimeRange SumColumn::GetTimeRange() const {
  return time_range_;
}

Column SumColumn::Extract() {
  auto sum_column = std::make_shared<SumColumn>(std::move(buckets_),
                                                time_range_, bucket_interval_);
  buckets_ = {};
  time_range_ = {};
  auto read_column = std::static_pointer_cast<IReadColumn>(sum_column);
  return std::static_pointer_cast<IColumn>(read_column);
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
  timestamps_column_->Write(time_series);
  values_column_->Write(time_series);
}

std::vector<Value> ReadRawColumn::GetValues() const {
  return values_column_->GetValues();
}

TimeRange ReadRawColumn::GetTimeRange() const {
  return TimeRange{timestamps_column_->timestamps_.front(),
                   timestamps_column_->timestamps_.back()};
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
  auto timestamps = timestamps_column_->GetValues();
  return {timestamps.begin(), timestamps.end()};
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

Column CreateColumn(ColumnType column_type, Duration bucket_interval) {
  switch (column_type) {
    case ColumnType::kSum: {
      auto sum_column = std::make_shared<SumColumn>(bucket_interval);
      // some strange things here, because in cpp we don't have such thing as
      // interface, so in case of diamond interface inheritance we need to
      // explicitly cast to the base class
      auto read_column = std::static_pointer_cast<IReadColumn>(sum_column);
      return std::static_pointer_cast<IColumn>(read_column);
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
      auto reader = CompressedBytesReader(bytes);
      auto bucket_interval = reader.Read<size_t>();
      auto start = reader.Read<TimePoint>();
      auto end = reader.Read<TimePoint>();
      auto buckets = reader.ReadAll<Value>();
      auto sum_column = std::make_shared<SumColumn>(
          buckets, TimeRange{start, end}, bucket_interval);
      auto read_column = std::static_pointer_cast<IReadColumn>(sum_column);
      return std::static_pointer_cast<IColumn>(read_column);
    }
    default:
      throw std::runtime_error("Unsupported column type");
  }
}

CompressedBytesReader::CompressedBytesReader(const CompressedBytes& bytes)
    : bytes_(bytes) {}

}  // namespace tskv
