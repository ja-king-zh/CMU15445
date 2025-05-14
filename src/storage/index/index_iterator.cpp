/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, size_t index, bool end) 
: bpm_(bpm), page_id_(page_id), index_(index), is_end_(end){

}


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  //throw std::runtime_error("unimplemented");
  return is_end_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  //throw std::runtime_error("unimplemented");
  auto guard = bpm_->WritePage(page_id_);
  auto res = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return std::make_pair(res->KeyAt(index_), res->ValueAt(index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  //throw std::runtime_error("unimplemented");
  auto guard = bpm_->WritePage(page_id_);
  auto res = guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  //std::cout << "page_size:" << res->GetSize() << std::endl;
  if (index_ + 1< res->GetSize()) {
    index_++;
  } else {
    page_id_ = res->GetNextPageId();
    index_ = 0;
    if (page_id_ == INVALID_PAGE_ID) {
      is_end_ = true;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
