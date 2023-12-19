#include "disk_storage.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>

namespace tskv {

DiskStorage::DiskStorage(const Options& options) : path_(options.path) {
  if (!std::filesystem::exists(path_)) {
    std::filesystem::create_directories(path_);
  }
}

DiskStorage::Metadata DiskStorage::GetMetadata() const {
  assert(false);
}

std::string DiskStorage::GeneratePageId() {
  // generate random uuidv4
  auto mt = std::mt19937{std::random_device{}()};
  std::string out;
  static std::string chars = "0123456789abcdef";
  for (auto i = 0; i < 36; ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      out += '-';
    } else {
      auto dist = std::uniform_int_distribution<>{0, 15};
      out += chars[dist(mt)];
    }
  }
  return out;
}

PageId DiskStorage::CreatePage() {
  auto page_id = GeneratePageId();
  std::ofstream out(path_ / page_id, std::ios::binary);
  while (out.fail()) {
    page_id = GeneratePageId();
    out.open(page_id);
  }
  out.close();
  return page_id;
}

CompressedBytes DiskStorage::Read(const PageId& page_id) {
  std::ifstream in(path_ / page_id, std::ios::binary);
  if (!in) {
    throw std::runtime_error("file not found");
  }
  CompressedBytes content = {(std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>()};
  return content;
}

void DiskStorage::Write(const PageId& page_id, const CompressedBytes& bytes) {
  std::ofstream out(path_ / page_id, std::ios::binary);
  if (!out) {
    throw std::runtime_error("file not found");
  }
  out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

void DiskStorage::DeletePage(const PageId& page_id) {
  std::filesystem::remove(path_ / page_id);
}
}  // namespace tskv
