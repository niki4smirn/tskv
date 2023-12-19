#include "level.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <utility>

#include "model/column.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

Level::Level(const Options& options,
             std::shared_ptr<IPersistentStorage> storage)
    : options_(options), storage_(std::move(storage)) {}

Column Level::Read(const TimeRange& time_range,
                   StoredAggregationType aggregation_type) const {
  if (page_ids_.empty()) {
    return {};
  }
  auto column_type = ToColumnType(aggregation_type);
  if (column_type == ColumnType::kRawRead) {
    return ReadRawValues(time_range);
  }
  auto it = std::ranges::find(page_ids_, column_type,
                              &std::pair<ColumnType, PageId>::first);
  assert(it != page_ids_.end());

  auto bytes = storage_->Read(it->second);
  auto column =
      std::static_pointer_cast<IReadColumn>(FromBytes(bytes, column_type));

  return column->Read(time_range);
}

Column Level::ReadRawValues(const TimeRange& time_range) const {
  auto ts_it = std::ranges::find(page_ids_, ColumnType::kRawTimestamps,
                                 &std::pair<ColumnType, PageId>::first);
  if (ts_it == page_ids_.end()) {
    return {};
  }
  auto vals_it = std::ranges::find(page_ids_, ColumnType::kRawValues,
                                   &std::pair<ColumnType, PageId>::first);
  assert(vals_it != page_ids_.end());
  auto ts_column = std::static_pointer_cast<RawTimestampsColumn>(
      FromBytes(storage_->Read(ts_it->second), ColumnType::kRawTimestamps));
  auto vals_column = std::static_pointer_cast<RawValuesColumn>(
      FromBytes(storage_->Read(vals_it->second), ColumnType::kRawValues));
  auto read_column = std::make_shared<ReadRawColumn>(ts_column, vals_column);

  return read_column->Read(time_range);
}

void Level::Write(const SerializableColumn& column) {
  if (!options_.store_raw &&
      (column->GetType() == ColumnType::kRawValues ||
       column->GetType() == ColumnType::kRawTimestamps)) {
    return;
  }
  if (auto read_col = std::dynamic_pointer_cast<IReadColumn>(column)) {
    auto time_range = read_col->GetTimeRange();
    time_range_ = time_range_.Merge(time_range);
  }
  auto column_type = column->GetType();
  auto it = std::ranges::find(page_ids_, column_type,
                              &std::pair<ColumnType, PageId>::first);
  if (it == page_ids_.end()) {
    PageId page_id = storage_->CreatePage();
    page_ids_.emplace_back(column_type, page_id);
    storage_->Write(page_id, column->ToBytes());
    return;
  }

  PageId& page_id = it->second;
  auto read_column = std::dynamic_pointer_cast<ISerializableColumn>(
      FromBytes(storage_->Read(page_id), column_type));
  read_column->Merge(column);
  storage_->DeletePage(page_id);
  page_id = storage_->CreatePage();
  storage_->Write(page_id, read_column->ToBytes());
}

void Level::MovePagesFrom(Level& other) {
  if (options_.bucket_interval == other.options_.bucket_interval &&
      options_.store_raw == other.options_.store_raw) {
    page_ids_.insert(page_ids_.end(), other.page_ids_.begin(),
                     other.page_ids_.end());
  } else {
    for (auto& [column_type, page_id] : other.page_ids_) {
      auto it = std::ranges::find(page_ids_, column_type,
                                  &std::pair<ColumnType, PageId>::first);
      if (it == page_ids_.end()) {
        if ((column_type == ColumnType::kRawTimestamps ||
             column_type == ColumnType::kRawValues) &&
            !options_.store_raw) {
          storage_->DeletePage(page_id);
        } else {
          page_ids_.emplace_back(column_type, page_id);
        }
        continue;
      }

      auto bytes = other.storage_->Read(page_id);
      auto column = std::dynamic_pointer_cast<ISerializableColumn>(
          FromBytes(bytes, column_type));
      if (column_type == ColumnType::kRawTimestamps ||
          column_type == ColumnType::kRawValues) {
        Write(column);
      } else {
        auto aggreagte_column =
            std::dynamic_pointer_cast<IAggregateColumn>(column);
        aggreagte_column->ScaleBuckets(options_.bucket_interval);
        Write(aggreagte_column);
      }
      other.storage_->DeletePage(page_id);
    }
  }

  time_range_ = time_range_.Merge(other.time_range_);

  other.page_ids_.clear();
  other.time_range_ = {};
}

bool Level::NeedMerge() const {
  return time_range_.GetDuration() >= options_.level_duration;
}

}  // namespace tskv
