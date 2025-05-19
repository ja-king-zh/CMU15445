#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) != current_reads_.end()) {
    current_reads_[read_ts]++;
  } else {
    current_reads_[read_ts] = 1;
    pq_.push(read_ts);
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
  }
  while (!pq_.empty() && current_reads_.find(pq_.top()) == current_reads_.end()) {
    pq_.pop();
  }
  if (pq_.empty()) {
    watermark_ = commit_ts_;
  } else {
    watermark_ = pq_.top();
  }
}

}  // namespace bustub
