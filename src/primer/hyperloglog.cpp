//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits)
    : cardinality_(0), v_(1 << std::max(0, static_cast<int>(n_bits)), 0), b_(n_bits) {}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> bs;
  size_t cnt = 0;
  size_t res = hash;
  while (res > 0) {
    if (res % 2 == 1) {
      bs.set(cnt);
    }
    cnt += 1;
    res /= 2;
  }
  // std::cout << bs << ' ' << hash << std::endl;
  return bs;
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  for (int i = BITSET_CAPACITY - b_ - 1, j = 1; i >= 0; i--, j++) {
    if (bset[i]) {
      return j;
    }
  }
  return 0;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hs = CalculateHash(val);
  auto bs = ComputeBinary(hs);
  auto res = PositionOfLeftmostOne(bs);
  int ans = 0;
  for (int i = BITSET_CAPACITY - 1, j = b_; j > 0; i--, j--) {
    if (bs[i] == 1) {
      ans += 1 << (j - 1);
    }
  }
  v_[ans] = std::max(v_[ans], static_cast<int>(res));
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  int m = v_.size();
  double res = 0;
  for (int i = 0; i < m; i++) {
    res += pow(2, -v_[i]);
  }
  double ans = CONSTANT * m * m / (res);
  cardinality_ = static_cast<size_t>(ans);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
