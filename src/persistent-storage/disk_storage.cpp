#include "disk_storage.h"
#include <cassert>
#include <fstream>
#include <stdexcept>
#include "model/column.h"

namespace tskv {

DiskStorage::DiskStorage(const Options&) {}

DiskStorage::Metadata DiskStorage::GetMetadata() const {
  // TODO: implement
  assert(false);
}

PageId DiskStorage::CreatePage() {
  auto page_id = next_page_id_;
  ++next_page_id_;
  // i'm not sure if this is the best way to create a file
  std::ofstream out(std::to_string(page_id));
  out.close();
  return page_id;
}

CompressedBytes DiskStorage::Read(const PageId& page_id) {
  std::ifstream in(std::to_string(page_id), std::ios::binary);
  if (!in) {
    throw std::runtime_error("file not found");
  }
  CompressedBytes content((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
  return content;
}

void DiskStorage::Write(const PageId& page_id, const CompressedBytes& bytes) {
  std::ofstream out(std::to_string(page_id), std::ios::binary);
  if (!out) {
    throw std::runtime_error("file not found");
  }
  out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

void DiskStorage::DeletePage(const PageId& page_id) {
  // TODO: implement
  assert(false);
}
}  // namespace tskv
