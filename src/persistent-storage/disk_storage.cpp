#include "disk_storage.h"

namespace tskv {

DiskStorage::DiskStorage(const Options&) {}

DiskStorage::Metadata DiskStorage::GetMetadata() const {}

std::shared_ptr<IPage> DiskStorage::Read(const PageId& page_id) {}

bool DiskStorage::Write(const PageId& page_id, std::shared_ptr<IPage> page) {}
}  // namespace tskv
