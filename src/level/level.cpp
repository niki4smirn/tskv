#include "level.h"
#include <algorithm>
#include "model/column.h"
#include "persistent-storage/persistent_storage.h"

namespace tskv {

Level::Level(std::shared_ptr<IPersistentStorage> storage) {
  storage_ = storage;
}

Column Level::Read(const TimeRange& time_range,
                   StoredAggregationType aggregation_type) {
  // we don't support raw columns, so simply cast
  auto column_type = static_cast<ColumnType>(aggregation_type);
  auto it = std::ranges::find(page_ids_, column_type,
                              &std::pair<ColumnType, PageId>::first);
  if (it == page_ids_.end()) {
    return {};
  }
  auto bytes = storage_->Read(it->second);
  Column result = FromBytes(bytes, column_type);
  return result->Read(time_range);
}

void Level::Write(const Column& column) {
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
