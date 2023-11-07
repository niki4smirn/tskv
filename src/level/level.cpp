#include "level.h"
#include <algorithm>
#include <cassert>
#include <memory>
#include <utility>
#include "model/column.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

Level::Level(std::shared_ptr<IPersistentStorage> storage) {
  storage_ = std::move(storage);
}

Column Level::Read(const TimeRange& time_range,
                   StoredAggregationType aggregation_type) {
  auto column_type = static_cast<ColumnType>(aggregation_type);
  auto it = std::ranges::find(page_ids_, column_type,
                              &std::pair<ColumnType, PageId>::first);
  if (it == page_ids_.end()) {
    return ReadRawValues(time_range, aggregation_type);
  }
  auto bytes = storage_->Read(it->second);
  auto column =
      std::static_pointer_cast<IReadColumn>(FromBytes(bytes, column_type));

  return column->Read(time_range);
}

Column Level::ReadRawValues(const TimeRange& time_range,
                            StoredAggregationType aggregation_type) {
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
  auto column_type = column->GetType();
  auto it = std::ranges::find(page_ids_, column_type,
                              &std::pair<ColumnType, PageId>::first);

  PageId page_id;
  if (it == page_ids_.end()) {
    page_id = storage_->CreatePage();
    page_ids_.emplace_back(column_type, page_id);
  } else {
    page_id = it->second;
  }
  storage_->Write(page_id, column->ToBytes());
}

}  // namespace tskv
