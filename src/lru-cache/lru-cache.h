#pragma once

#include <list>
#include <string>
#include <unordered_map>

template <typename Key, typename Value>
class LruCache {
 public:
  explicit LruCache(size_t max_size) : max_size_(max_size) {}

  void Set(const Key& key, const Value& value) {
    if (values_.contains(key)) {
      MoveToEnd(key);
    } else {
      if (queue_.size() == max_size_) {
        const std::string& deleting_key = queue_.front();
        values_.erase(deleting_key);
        key_to_iter_.erase(deleting_key);
        queue_.pop_front();
      }
      AddNewKey(key);
    }
    values_[key] = value;
  }

  bool Get(const Key& key, Value* value) {
    auto value_it = values_.find(key);
    if (value_it == values_.end()) {
      return false;
    }
    MoveToEnd(key);
    *value = value_it->second;
    return true;
  }

 private:
  void MoveToEnd(const Key& key) {
    queue_.erase(key_to_iter_[key]);
    AddNewKey(key);
  }

  void AddNewKey(const Key& key) {
    queue_.push_back(key);
    key_to_iter_[key] = --queue_.end();
  }

  std::unordered_map<Key, Value> values_;
  std::unordered_map<Key, typename std::list<Key>::iterator> key_to_iter_;
  std::list<Key> queue_;
  size_t max_size_;
};
