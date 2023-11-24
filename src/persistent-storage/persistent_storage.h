#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "model/column.h"

namespace tskv {

using PageId = std::string;

class IPersistentStorage {
 public:
  struct Metadata {};

 public:
  virtual ~IPersistentStorage() = default;

  virtual Metadata GetMetadata() const = 0;
  virtual PageId CreatePage() = 0;
  virtual CompressedBytes Read(const PageId& page_id) = 0;
  virtual void Write(const PageId& page_id, const CompressedBytes& bytes) = 0;
  virtual void DeletePage(const PageId& page_id) = 0;
};

}  // namespace tskv
