//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

// we allocate a consecutive memory space for the buffer pool
// 我们为缓冲池分配一个连续的内存空间
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  // 初始化的时候，每个页面都在空闲列表中。
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].pin_count_ = 0;
    pages_[i].is_dirty_ = false;
    pages_[i].page_id_ = 0;
  }

  /// TODO:(students): remove this line after you have implemented the buffer
  /// pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished
  //     implementing BPM, please remove the throw " "exception line in
  //     `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // pages_[frame_id].WLatch();

    // 这里不脏，从free_list_里面拿出来的都不脏。
    pages_[frame_id].pin_count_ = 1;
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].page_id_ = *page_id;

    // pages_[frame_id].WUnlatch();
    return pages_ + frame_id;
  }

  if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  *page_id = AllocatePage();
  page_table_->Remove(pages_[frame_id].page_id_);
  page_table_->Insert(*page_id, frame_id);

  // pages_[frame_id].WLatch();

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }

  // 这里不做修改。
  pages_[frame_id].ResetMemory();

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].page_id_ = *page_id;

  // pages_[frame_id].WUnlatch();
  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    // pages_[frame_id].WLatch();
    pages_[frame_id].is_dirty_ = true;
    ++pages_[frame_id].pin_count_;
    // pages_[frame_id].WUnlatch();

    return pages_ + frame_id;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();

    // pages_[frame_id].WLatch();
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    // pages_[frame_id].WUnlatch();

    page_table_->Insert(page_id, frame_id);

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return pages_ + frame_id;
  }

  if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  page_table_->Remove(pages_[frame_id].page_id_);
  page_table_->Insert(page_id, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // pages_[frame_id].WLatch();

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }

  pages_[frame_id].ResetMemory();
  ++pages_[frame_id].pin_count_;
  pages_[frame_id].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  // pages_[frame_id].WUnlatch();
  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  // pages_[frame_id].WLatch();

  if (pages_[frame_id].pin_count_ <= 0) {
    // pages_[frame_id].WUnlatch();
    return false;
  }

  // 超你妈的，#p2老是读脏数据才怀疑是这里错了。

  if (!pages_[frame_id].is_dirty_) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  // pages_[frame_id].WUnlatch();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  frame_id_t frame_id;

  if (page_table_->Find(page_id, frame_id)) {
    // pages_[frame_id].WLatch();
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    // pages_[frame_id].WUnlatch();
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  for (page_id_t i = 0, j = -1; i < next_page_id_; ++i) {
    if (page_table_->Find(i, j)) {
      // pages_[j].WLatch();
      disk_manager_->WritePage(i, pages_[j].data_);
      pages_[j].is_dirty_ = false;
      // pages_[j].WUnlatch();
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::shared_mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  // pages_[frame_id].WLatch();
  if (pages_[frame_id].pin_count_ > 0) {
    // pages_[frame_id].WUnlatch();
    return false;
  }

  // 脏了，写回去。
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }

  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = 0;

  page_table_->Remove(page_id);
  replacer_->SetEvictable(frame_id, true);
  // replacer_->RecordAccess(frame_id); 打错了，应该是Remove
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  // pages_[frame_id].WUnlatch();
  return true;
}

// 仅在内部使用，无需上锁。
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
