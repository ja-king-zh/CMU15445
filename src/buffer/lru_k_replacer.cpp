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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// 驱逐与 replacer_ 跟踪的所有其他可驱逐帧相比具有最大后向 k 距离的帧。如果没有可驱逐帧，则返回 std::nullopt。
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::unique_lock<std::mutex> lock(latch_);
  // 如果replacer中没有frame，返回空
  if (replacer_.empty()) {
    return std::nullopt;
  }
  // 找到一个淘汰的frame
  auto it = replacer_.begin();
  auto node = *it;
  auto fid = node->fid_;
  // 将其从node_store_中删除
  node_store_.erase(fid);
  // 将其从replacer中删除
  replacer_.erase(it);
  // 减小node_store_实际大小
  curr_size_--;
  return fid;
}

// 记录给定帧在当前时间戳已被访问。应在页面被固定在 BufferPoolManager 中后调用此方法。
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    // 抛出异常
    return;
  }
  current_timestamp_++;
  auto it = node_store_.find(frame_id);
  // 如果当前frame不在node_store_中，将其加入
  if (it == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(k_, frame_id));
    it = node_store_.find(frame_id);
  }
  auto node = &(it->second);
  if (node->is_evictable_) {
    replacer_.erase(node);
  }
  // 保证历史记录只有k个
  if (node->history_.size() == k_) {
    node->history_.pop_front();
  }
  // 更新history
  node->history_.push_back(current_timestamp_);
  //更新replacer
  if (node->is_evictable_) {
    replacer_.insert(node);
  }
}

/* 此方法控制框架是否可驱逐。它还控制 LRUKReplacer 的大小。当您实现 BufferPoolManager 时，
 * 您将知道何时调用此函数。具体来说，当页面的引脚数达到 0 时，其对应的框架应标记为可驱逐。
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // 抛出异常
    return;
  }
  auto node = &(it->second);
  if (node->is_evictable_ != set_evictable) {
    if (set_evictable) {
      curr_size_++;
      // 将这个frame放入replacer
      replacer_.insert(node);
    } else {
      curr_size_--;
      // 将这个frame移出replacer
      replacer_.erase(node);
    }
  }
  node->is_evictable_ = set_evictable;
}

// 清除与帧相关的所有访问历史记录。仅当在 BufferPoolManager 中删除页面时才应调用此方法。
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto node = &(node_store_.find(frame_id)->second);
  if (!node->is_evictable_) {
    //抛出异常
    return;
  }
  // 清除replacer
  replacer_.erase(node);
  // node_store_中清除这个frame
  node_store_.erase(frame_id);
  //  减小node_store_的实际大小
  curr_size_--;
}

// 此方法返回当前位于 LRUKReplacer 中的可驱逐框架的数量。
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub