//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// ddddd
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * @brief Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the lookback constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable
   * logging). Please ignore this for P1.
   *
   * 创建一个新的缓冲管理器实例。
   * replacer_k 就是 LRU-k 的 k
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManagerInstance.
   * 销毁
   */
  ~BufferPoolManagerInstance() override;

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t override { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  // 返回缓存池中的所有页面。
  auto GetPages() -> Page * { return pages_; }

 protected:
  /**
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's
   * id, or nullptr if all frames are currently in use and not evictable (in
   * another word, pinned).
   *
   * 在缓存池中新建一个页面。为新页面设置一个页面id。
   * 否则返回nullptr，如果所有帧都在并行使用中并且不可驱逐（或者说，被钉住了）。
   *
   * You should pick the replacement frame from either the free list or the
   * replacer (always find from the free list first), and then call the
   * AllocatePage() method to get a new page id. If the replacement frame has a
   * dirty page, you should write it back to the disk first. You also need to
   * reset the memory and metadata for the new page.
   *
   * 你要么从空闲列表中，要么从替代器（总是从空列表的第一个）中挑选一个替代帧，然后调用AllocatePage()模式来获得一个新页面id。
   * 如果替代帧有一个脏页面，你应该首先把它写回磁盘。你也需要重设内存和元数据这个新页面。
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id,
   * false) so that the replacer wouldn't evict the frame before the buffer pool
   * manager "Unpin"s it. Also, remember to record the access history of the
   * frame in the replacer for the lru-k algorithm to work.
   * 
   * 记住Pin这个帧，通过调用替代器（SetEvictable(frame_id,false)）
   * 这样替代器不会驱逐这个帧在缓冲管理器Unpin这个帧
   * 同时，记得用替代器记录到达历史，以便让lru-k算法工作。
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new
   * page 返回：如果新页面没有创建则返回空，否则，返回一个指向新页面的指针。
   */
  auto NewPgImp(page_id_t *page_id) -> Page * override;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if
   * page_id needs to be fetched from the disk but all frames are currently in
   * use and not evictable (in another word, pinned).
   *
   * 从缓冲池中抓取需要的页面。如果所需要的页面在磁盘中，但是所有帧都在并行使用且是not
   * evictable（或者被顶住了） 则返回nullptr
   *
   * First search for page_id in the buffer pool. If not found, pick a
   * replacement frame from either the free list or the replacer (always find
   * from the free list first), read the page from disk by calling
   * disk_manager_->ReadPage(), and replace the old page in the frame. Similar
   * to NewPgImp(), if the old page is dirty, you need to write it back to disk
   * and update the metadata of the new page
   *
   * 第一次在缓冲池中搜索页面id。如果找不到，那么挑选一个替代帧，要么从空列表中，要么从替代器中，
   * 然后阅读这个页面通过调用disk_manage_->ReadPage()，然后替代帧中的旧页面。
   * 类似于NewPgImp()，如果旧页面是脏的，你需要写回磁盘，并且更新新页面中的元数据。
   *
   * In addition, remember to disable eviction and record the access history of
   * the frame like you did for NewPgImp().
   *
   * 此外，记住不可驱逐和记录帧的到达历史就像你在NewPgImp()中所作的。
   *
   * @param page_id id of page to be fetched
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the
   * requested page
   *
   * 返回nullptr如果页面id不可抓取，否则返回指向所需要页面的指针。
   */
  auto FetchPgImp(page_id_t page_id) -> Page * override;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the
   * buffer pool or its pin count is already 0, return false.
   *
   * 从缓冲池中Unpin目标页面。如果页面id不在缓冲池或者它的pin
   * count已经为0，则返回false。
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame
   * should be evictable by the replacer. Also, set the dirty flag on the page
   * to indicate if the page was modified.
   *
   * 减少一个页面的pin count。如果pin count达到0，这个帧应该被replacer驱逐。
   * 同时，如果页面被修改，则设置脏页面flag来表示
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0
   * before this call, true otherwise 返回false如果页面不在page
   * table中或者它的pin count已经<=0在调用这个函数之前，否则返回true。
   */
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * 把目标页面刷新到磁盘上。
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS
   * of the dirty flag. Unset the dirty flag of the page after flushing.
   *
   * 使用DiskManager::WritePage()模式来吧一个页面刷新到磁盘上，不管脏标记。
   * 把脏标记拿下在刷新之后。
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true
   * otherwise
   *
   * 返回false如果页面在page table中找不到，否则返回true。
   */
  auto FlushPgImp(page_id_t page_id) -> bool override;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   *
   * 把所有在缓冲池中的页面刷新到磁盘中。
   *
   */
  void FlushAllPgsImp() override;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer
   * pool, do nothing and return true. If the page is pinned and cannot be
   * deleted, return false immediately.
   *
   * 从缓冲池中删除一个页面。如果页面id不在缓冲池中，什么也不做并返回true。
   * 如果这个页面被顶住了并且不能删除，立刻返回false。
   *
   * After deleting the page from the page table, stop tracking the frame in the
   * replacer and add the frame back to the free list. Also, reset the page's
   * memory and metadata. Finally, you should call DeallocatePage() to imitate
   * freeing the page on the disk.
   *
   * 在把页面从page
   * table中删除页面之后，停止跟踪这个帧在替代器中并把这个帧添加到空列表中。
   * 同时，重置页面的内存和元数据。最终，你应该调用DellocatePage()来模仿在磁盘中释放这个页面。
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page
   * didn't exist or deletion succeeded
   */
  auto DeletePgImp(page_id_t page_id) -> bool override;

  auto GetAvaibleFrame(frame_id_t *res) -> bool;
  /** Number of pages in the buffer pool. */
  // 缓冲池中的页面个数。
  const size_t pool_size_;
  /** The next page id to be allocated  */
  // 下一个即将被分配的页面id。
  std::atomic<page_id_t> next_page_id_ = 0;
  /** Bucket size for the extendible hash table */
  // 给extendible hash table的桶的个数。
  const size_t bucket_size_ = 4;

  /** Array of buffer pool pages. */
  // 缓冲池页面数组
  Page *pages_;
  /** Pointer to the disk manager. */
  // 指向disk manager的指针
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  // 指向日志管理器，在p1中请忽略。
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  // page table来跟踪缓冲池页面。
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;
  /** Replacer to find unpinned pages for replacement. */
  // 替代器，来寻找unpinned页面来替代。
  LRUKReplacer *replacer_;
  /** List of free frames that don't have any pages on them. */
  // 空闲帧的列表，里面不含任何页面。
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this
   * comment to describe what it protects. */
  // 此锁存可保护共享数据结构。我们建议更新此注释以描述它保护的内容。

  std::shared_mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before
   * calling this function.
   * 分配一个页面在磁盘上。调用者应该申请一个锁在调用这个函数之前。
   * @return the id of the allocated page
   * 返回分配页面的id。
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before
   * calling this function.
   * @param page_id id of the page to deallocate
   *
   * 释放一个在磁盘上的页面。条用着应该申请一个锁，在调用这个函数之前。
   *
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track
    // deallocated pages
  }

  // TODO(student): You may add additional private members and helper functions
};
}  // namespace bustub
