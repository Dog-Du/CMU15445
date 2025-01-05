//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : total_size_(num_frames), k_(k), replacer_(total_size_, std::make_pair(std::list<size_t>(), false)) {}

// 选一个驱逐，然后清理，默认设置false
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  // 用两个变量是为了区别 小于k个记录 的情况。
  size_t max_interval = 0;
  size_t eariest_time = INT64_MAX;
  bool flag = false;

  auto j = replacer_.begin();  // 指向需要删除迭代器。

  // n为帧的个数。
  for (auto it = replacer_.begin(); it != replacer_.end(); ++it) {
    if (!it->second || it->first.empty()) {
      continue;
    }

    flag = true;

    // 小于 k 个。
    if (it->first.size() < k_) {
      max_interval = INT64_MAX;

      // 需要eariest_time
      if (it->first.front() < eariest_time) {
        *frame_id = it - replacer_.begin();
        eariest_time = it->first.front();
        j = it;
      }
    } else {
      if (current_timestamp_ - it->first.front() > max_interval) {
        *frame_id = it - replacer_.begin();
        max_interval = current_timestamp_ - it->first.front();
        j = it;
      }
    }
  }

  // 清空记录，默认设置成false
  if (flag) {
    j->first.clear();
    j->second = false;
    --replacer_size_;
  }

  return flag;
}

// 增加一个达到记录，但是不改变evictable，也不改变replacer_size_
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  // 断言，
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < total_size_, "frame_id is over in the function LRUKReplacer::RecordAccess");

  auto j = replacer_.begin() + frame_id;
  // std::cout<< mp_.size() << std::endl;

  if (j->first.size() == k_) {
    j->first.pop_front();
  }

  j->first.push_back(current_timestamp_++);
}

// 设置evictable,改变replacer_size_,但是不清空。
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  // 断言，不在范围，则abort
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < total_size_, "frame_id is over in the function LRUKReplacer::SetEvictable");

  auto j = replacer_.begin() + frame_id;
  if ((j->first.empty())) {
    return;
  }

  if (set_evictable && !j->second) {
    ++replacer_size_;
  }

  if (!set_evictable && j->second) {
    --replacer_size_;
  }

  j->second = set_evictable;
}

// 删除一个evictable的，设为false,清空。
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  auto j = replacer_.begin() + frame_id;
  // id不在范围。
  if (frame_id < 0 || frame_id >= total_size_) {
    return;
  }

  // 断言。如果unevictable，不进行删除

  if (!j->first.empty()) {
    // j->second==false的时候才abort，之前写成了!j->second,导致oj的时候直接出现
    // ： subprocess abort() 了。
    BUSTUB_ASSERT(j->second,
                  "trying Remove an unevictable frame in the function "
                  "LRUReplacer::Remove");

    j->first.clear();
    j->second = false;
    --replacer_size_;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return replacer_size_;
}

}  // namespace bustub
