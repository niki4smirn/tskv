#pragma once

#include <memory>
#include <string>

namespace tskv {

class PageId;
class IPage;

class IPersistentStorage {
 public:
  struct Metadata {};

 public:
  virtual ~IPersistentStorage() = default;

  virtual Metadata GetMetadata() const = 0;
  virtual std::shared_ptr<IPage> Read(const PageId& page_id) = 0;
  // returns true on success, false otherwise
  virtual bool Write(const PageId& page_id, std::shared_ptr<IPage> page) = 0;
};

}  // namespace tskv
