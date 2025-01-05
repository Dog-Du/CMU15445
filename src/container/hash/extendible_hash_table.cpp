//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"

#include <cassert>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <list>
#include <utility>

#include "storage/page/page.h"

namespace bustub {

// 让dir_初始化的时候有一个桶。
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::shared_ptr<Bucket>(new Bucket(bucket_size)));
}

// mask表示下位数，这样IndexOf(key)之后绝对不会超出字典的大小。
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  return std::hash<K>()(key) & ((1 << global_depth_) - 1);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::shared_mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::shared_mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::shared_mutex> lock(latch_);
  // 这里不可写成 IndexOf()
  // 因为这里申请了互斥锁，但是IndexOf()为共享锁。会造成死锁，下面同理。，
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  size_t index = IndexOf(key);

  if (dir_[index]->Insert(key, value)) {
    return;
  }

  while (dir_[index]->IsFull()) {
    const int local_depth = dir_[index]->GetDepth();
    const int local_mask = 1 << local_depth;

    if (local_depth == global_depth_) {
      size_t n = dir_.size();
      ++global_depth_;

      dir_.reserve(2 * n);
      std::copy_n(dir_.begin(), n, std::back_inserter(dir_));
    }

    dir_[index]->IncrementDepth();
    std::shared_ptr<Bucket> new_bucket(new Bucket(bucket_size_, local_depth + 1));
    ++num_buckets_;

    std::list<std::pair<K, V>> &new_zero_list = new_bucket->GetItems();
    std::list<std::pair<K, V>> &old_one_list = dir_[index]->GetItems();

    for (auto it = old_one_list.begin(); it != old_one_list.end();) {
      if ((std::hash<K>()(it->first) >> local_depth & 1) == 0) {
        new_zero_list.splice(new_zero_list.end(), old_one_list, it++);
      } else {
        ++it;
      }
    }

    // 最后的最后debug才发现是这里错误了。必须是遍历整个桶，然后改变方向。
    for (int i = std::hash<K>()(key) & (local_mask - 1), n = static_cast<int>(dir_.size()); i < n; i += local_mask) {
      if (((i >> local_depth) & 1) == 0) {
        dir_[i] = new_bucket;
      }
    }

    index = IndexOf(key);
  }

  dir_[index]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (key == k) {
      value = v;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (key == k) {
      v = value;
      return true;
    }
  }

  if (!IsFull()) {
    list_.emplace_front(key, value);
    return true;
  }

  return false;  // 返回false说明一定已经满了。
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
