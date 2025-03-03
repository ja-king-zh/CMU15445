//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : dense_bucket_(1 << std::max(0, static_cast<int>(n_leading_bits)), std::bitset<DENSE_BUCKET_SIZE>()),
      cardinality_(0),
      b_(n_leading_bits),
      v_(1 << std::max(0, static_cast<int>(n_leading_bits)), 0) {}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hs = CalculateHash(val);
  int cnt = 0;
  int ans = 0;
  int y = 0;
  std::bitset<64> bs;
  while (hs > 0) {
    if (hs % 2 == 1) {
      bs.set(cnt);
    }
    cnt += 1;
    hs /= 2;
  }
  for (y = 0; y < 64 - b_; y++) {
    if (bs[y]) {
      break;
    }
  }
  for (int i = 64 - 1, j = b_; j > 0; i--, j--) {
    if (bs[i]) {
      ans += 1 << (j - 1);
    }
  }
  if (y > v_[ans]) {
    v_[ans] = y;
    int a = y & 15;
    int b = y / 16;
    dense_bucket_[ans] = std::bitset<DENSE_BUCKET_SIZE>(a);
    overflow_bucket_[ans] = std::bitset<OVERFLOW_BUCKET_SIZE>(b);
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  int m = v_.size();
  double res = 0;
  for (int i = 0; i < m; i++) {
    res += pow(2, -v_[i]);
  }
  double ans = CONSTANT * m * m / (res);
  cardinality_ = static_cast<size_t>(ans);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
