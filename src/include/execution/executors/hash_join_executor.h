//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

#include <common/util/hash_util.h>

namespace bustub {
/** HashJoinKey represents a key in an aggregation operation */
struct HashJoinKey {
 /** The group-by values */
 std::vector<Value> group_bys_;

 /**
  * Compares two aggregate keys for equality.
  * @param other the other aggregate key to be compared with
  * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
  */
 auto operator==(const HashJoinKey &other) const -> bool {
  for (uint32_t i = 0; i < other.group_bys_.size(); i++) {
   if (group_bys_[i].CompareEquals(other.group_bys_[i]) != CmpBool::CmpTrue) {
    return false;
   }
  }
  return true;
 }
};
/** HashJoinValue represents a value for each of the running aggregates */
struct HashJoinValue {
 /** The aggregate values */
 std::vector<Tuple> aggregates_;
};
}

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
 auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
  size_t curr_hash = 0;
  for (const auto &key : agg_key.group_bys_) {
   if (!key.IsNull()) {
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
   }
  }
  return curr_hash;
 }
};
}



namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleHashJoinTable {
 public:

 void Insert(const HashJoinKey& key, Tuple& value) {
  if (ht_.find(key) == ht_.end()) {
   HashJoinValue new_value = HashJoinValue{std::vector<Tuple>{value}};
   ht_[key] = new_value;
  }else {
   //auto val = ht_[key];
   ht_[key].aggregates_.push_back(value);

  }
 }

 auto GetValue(const HashJoinKey& key) ->HashJoinValue {
  if (ht_.find(key) == ht_.end()) {
   return {};
  }
  return ht_[key];
 }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const HashJoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const HashJoinValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<HashJoinKey, HashJoinValue> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto MakeLeftJoinKey(const Tuple* tuple) -> HashJoinKey{
   std::vector<Value> values;
   for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
    values.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
   }
   return HashJoinKey{values};
  }
  auto MakeRightJoinKey(const Tuple* tuple) -> HashJoinKey{
   std::vector<Value> values;
   for (const auto &expr : plan_->RightJoinKeyExpressions()) {
    values.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
   }
   return HashJoinKey{values};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  bool has_joined_;
  Tuple pre_left_tuple;
  u_int32_t right_index;
  std::unique_ptr<SimpleHashJoinTable> mp_;

};

}  // namespace bustub


