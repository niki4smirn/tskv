#include "column.h"
#include <algorithm>
#include <cassert>

#include <ranges>

namespace tskv {

SumColumn::SumColumn(size_t bucket_interval)
    : bucket_interval_(bucket_interval) {}

SumColumn::SumColumn(std::vector<double> buckets, const TimeRange& time_range,
                     size_t bucket_interval)
    : buckets_(std::move(buckets)),
      time_range_(time_range),
      bucket_interval_(bucket_interval) {}

ColumnType SumColumn::GetType() const {
  return ColumnType::kSum;
}

CompressedBytes SumColumn::ToBytes() const {
  return {};
}

// column interval should start further than this->time_range.start
void SumColumn::Merge(Column column) {
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

Column SumColumn::Read(const TimeRange& time_range) const {
  auto start_bucket = *GetBucketIdx(time_range.start);
  auto end_bucket = *GetBucketIdx(time_range.end);
  return std::make_shared<SumColumn>(
      std::vector<double>(buckets_.begin() + start_bucket,
                          buckets_.begin() + end_bucket),
      time_range, bucket_interval_);
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
  if (timestamp < time_range_.start || timestamp > time_range_.end) {
    return {};
  }

  return (timestamp - time_range_.start) / bucket_interval_;
}

std::vector<Value> SumColumn::GetValues() const {
  return buckets_;
}

Column FromBytes(const CompressedBytes& bytes, ColumnType column_type) {
  // TODO: implement
  assert(false);
}
}  // namespace tskv
