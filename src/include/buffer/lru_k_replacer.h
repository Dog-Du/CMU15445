//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 * LRUKReplacer 实现一个LRU-K替代策略
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time
 * between current timestamp and the timestamp of kth previous access.
 *
 * LRU-k 算法逐出后向 k 距离为所有帧中最大值的帧。
 * 向后 k 距离计算为当前时间戳与前 k 次访问的时间戳之间的时间差。
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward
 * k-distance, classical LRU algorithm is used to choose victim.
 *
 * 一个帧如果少于k个历史引用，则会被给出+inf作用它的后 k 距离。如果多个真有 +inf
 * 的 后 k 距离。 则使用经典的LRU算法作用被驱逐者。
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be
   * required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that
   * frame. Only frames that are marked as 'evictable' are candidates for
   * eviction.
   *
   * 寻找一个帧具有最大的 后 k
   * 距离，然后把这个帧驱逐。只有被标记为可驱逐的帧才是驱逐的候选者。
   *
   * A frame with less than k historical references is given +inf as its
   * backward k-distance. If multiple frames have inf backward k-distance, then
   * evict the frame with the earliest timestamp overall.
   *
   * 一个帧少于 k 个历史引用，则其 后 k 距离是+inf。
   * 如果多个帧是的后k距离是+inf，则驱逐具有最早时间戳的帧。
   *
   * Successful eviction of a frame should decrement the size of replacer and
   * remove the frame's access history.
   *
   * 成功驱逐一个帧之后，应该减少替代器的大小，然后删除这个帧的达到历史。
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be
   * evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current
   * timestamp. Create a new entry for access history if frame id has not been
   * seen before.
   *
   * 记录所给帧id的事件为当前时间戳。创建一个新的entry给达到历史，如果该帧在之前从来没有使用过。
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an
   * exception. You can also use BUSTUB_ASSERT to abort the process if frame id
   * is invalid.
   *
   * 如果所给帧id是非法的，比如大于replacer_size_，则抛出一个异常。
   * 你也可以使用BUSTUB_ASSERT来终止进程，如果帧id是非法的。
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function
   * also controls replacer's size. Note that size is equal to number of
   * evictable entries.
   *
   * 切换一个帧是否是可驱逐的或者不可驱逐的。这个函数也控制replacer的大小。注意，replacer的大小等于可驱逐的个数。
   *
   * If a frame was previously evictable and is to be set to non-evictable, then
   * size should decrement. If a frame was previously non-evictable and is to be
   * set to evictable, then size should increment.
   *
   * 如果一个帧之前是可驱逐的并且即将被设置为不可驱逐的，那么replacer的大小应该减少；
   * 如果一个帧之前是不可驱逐的并且即将被设置为可驱逐的，那么replacer的大小应该增加。
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * 如果帧id是非法的，抛出一个异常或者终止进程。
   *
   * For other scenarios, this function should terminate without modifying
   * anything.
   *
   * 对于其他情况，这个函数应该终止，同时什么也不修改。
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access
   * history. This function should also decrement replacer's size if removal is
   * successful.
   *
   * 从replacer中删除一个可驱逐帧，根据它的到达历史。
   * 这个函数同时减少replacer的大小，如果删除成功。
   *
   * Note that this is different from evicting a frame, which always remove the
   * frame with largest backward k-distance. This function removes specified
   * frame id, no matter what its backward k-distance is.
   *
   * 注意，这和驱逐一个帧不同，那会选择一个最大后 k
   * 距离的帧去驱逐。这个函数删除给定的帧id，不管它的后 k 距离是多少。
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort
   * the process.
   *
   * 如果删除被调用与一个不可驱逐帧上，抛出一个异常或者终止进程。
   *
   * If specified frame is not found, directly return from this function.
   *
   * 如果给定帧找不到，直接返回。
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * 返回replacer的大小，这指可驱逐帧的个数。
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you
  // like. Remove maybe_unused if you start using them.

  // 去掉了cur_size_，不知道有什么用。

  size_t current_timestamp_{0};  // 时间戳
  frame_id_t total_size_;        // 总的frame的个数，最多有多少。
  size_t replacer_size_{0};      // evictable 的frame的 个数
  size_t k_;                     // k
  std::shared_mutex latch_;

  // replacer_list_存储 evictable 和 队列。evictable默认为false
  std::vector<std::pair<std::list<size_t>, bool>> replacer_;
};

}  // namespace bustub
