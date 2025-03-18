#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBPlusTreePage(page_id_t ans, Context &ctx) -> BPlusTreePage* {
  WritePageGuard wg = bpm_->WritePage(ans);
  auto res = wg.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(wg));
  ctx.page_id_ = ans;

  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(BPlusTreePage* res, Context &ctx, const KeyType &key) -> BPlusTreePage* {
  while (!res->IsLeafPage()) {
    auto cur = reinterpret_cast<InternalPage*>(res);
    int i = 1;
    while (i < cur->GetSize()) {
      if (comparator_(cur->KeyAt(i), key) > 0) {
        auto ans = cur->ValueAt(i - 1);
        res = GetBPlusTreePage(ans, ctx);
        break;
      }
      i++;
    }
    if (i == cur->GetSize()) {
      auto ans = cur->ValueAt(i - 1);
      res = GetBPlusTreePage(ans, ctx);
    }
  }
  if (ctx.write_set_.size() > 1) {
    ctx.write_set_.pop_back();
  }
  return res;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CtxInit(Context &ctx, WritePageGuard guard) {
  auto root_page_pre = guard.As<BPlusTreeHeaderPage>();
  WritePageGuard root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
  //ctx.header_page_ = std::move(guard);
  ctx.root_page_id_ = root_page_pre->root_page_id_;
  ctx.page_id_ = root_page_pre->root_page_id_;
  ctx.write_set_.push_back(std::move(root_guard));
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  if (IsEmpty()) {
    return false;
  }
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page_pre = guard.As<BPlusTreeHeaderPage>();
  WritePageGuard root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
  auto root_page = root_guard.AsMut<BPlusTreePage>();
  auto res = root_page;
  Context ctx;
  //CtxInit(ctx, std::move(guard));
  ctx.root_page_id_ = root_page_pre->root_page_id_;
  ctx.page_id_ = root_page_pre->root_page_id_;
  ctx.write_set_.push_back(std::move(root_guard));

  auto ans = GetLeafPage(res, ctx, key);
  auto cur = reinterpret_cast<LeafPage*>(ans);
  for (int i = 0; i < cur->GetSize(); i++) {
    if (comparator_(cur->KeyAt(i), key) == 0) {
      //TODO
      result->push_back(cur->ValueAt(i));
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  //已经有该key了
  std::vector<ValueType> result;
  if (GetValue(key, &result)) {
    return false;
  }
  Context ctx;
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::move(guard);
  auto root_page_pre = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  //当前树为空
  if (root_page_pre->root_page_id_ == INVALID_PAGE_ID) {
    root_page_pre->root_page_id_ = bpm_->NewPage();
    WritePageGuard root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
    auto res = root_guard.AsMut<BPlusTreePage>();
    auto ans = reinterpret_cast<LeafPage*>(res);
    ans->Init(leaf_max_size_);
    ans->ChangeSizeBy(1);
    ans->SetKeyAt(0, key);
    ans->SetValueAt(0, value);
    return true;
  }

  //初始化
  WritePageGuard root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
  auto res = root_guard.AsMut<BPlusTreePage>();
  //CtxInit(ctx, std::move(guard));
  ctx.root_page_id_ = root_page_pre->root_page_id_;
  ctx.page_id_ = root_page_pre->root_page_id_;
  ctx.write_set_.push_back(std::move(root_guard));

  auto leaf_page = GetLeafPage(res, ctx, key);
  auto cur = reinterpret_cast<LeafPage*>(leaf_page);
  //当前叶子空间足够，不需要分裂
  if (cur->GetSize() < cur->GetMaxSize()) {
    cur->SetSize(cur->GetSize() + 1);
    bool flag = true;
    for (int i = cur->GetSize() - 1; i > 0; i--) {
      if (comparator_(cur->KeyAt(i - 1), key) < 0) {
        cur->SetKeyAt(i, key);
        cur->SetValueAt(i, value);
        flag = false;
        break;
      } else {
        cur->SetKeyAt(i, cur->KeyAt(i - 1));
        cur->SetValueAt(i, cur->ValueAt(i - 1));
      }
    }
    if (flag) {
      cur->SetKeyAt(0, key);
      cur->SetValueAt(0, value);
    }
    return true;
  }

  auto new_page_id_ = bpm_->NewPage();
  auto new_page_guard = bpm_->WritePage(new_page_id_);
  auto new_page = new_page_guard.AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);

  std::vector<KeyType> v(cur->GetSize() + 1);
  std::vector<ValueType> v2(cur->GetSize() + 1);
  for (int i = 0; i < cur->GetSize(); i++) {
    v[i] = cur->KeyAt(i);
    v2[i] = cur->ValueAt(i);
  }
  bool flag = true;
  for (int i = cur->GetSize(); i > 0; i--) {
    if (comparator_(v[i - 1], key) > 0) {
      v[i] = v[i - 1];
      v2[i] = v2[i - 1];
    } else {
      v[i] = key;
      v2[i] = value;
      flag = false;
      break;
    }
  }
  if (flag) {
    v[0] = key;
    v2[0] = value;
  }
  int mid = (cur->GetSize() + 1) / 2;
  cur->SetSize(mid);
  for (int i = 0; i < mid; i ++ ) {
    cur->SetKeyAt(i, v[i]);
    cur->SetValueAt(i, v2[i]);
  }

  new_page->SetSize(v.size() - mid);
  for (size_t i = mid; i < v.size(); i++) {
    new_page->SetKeyAt(i - mid, v[i]);
    new_page->SetValueAt(i - mid, v2[i]);
  }
  new_page->SetNextPageId(cur->GetNextPageId());
  cur->SetNextPageId(new_page_id_);
  if (ctx.IsRootPage(ctx.page_id_)) {
    auto new_page_id_2 = bpm_->NewPage();
    auto new_page_guard2 = bpm_->WritePage(new_page_id_2);
    auto new_page2 = new_page_guard2.AsMut<InternalPage>();
    new_page2->Init(internal_max_size_);
    new_page2->SetSize(2);
    new_page2->SetKeyAt(1, new_page->KeyAt(0));
    new_page2->SetValueAt(1, new_page_id_);
    new_page2->SetValueAt(0, ctx.page_id_);
    root_page_pre->root_page_id_ = new_page_id_2;
  } else {
    auto bk_gard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto fa = bk_gard.AsMut<InternalPage>();
    ctx.page_id_ = bk_gard.GetPageId();
    //InsertInterval(cur, key, value, ctx);
    InsertInterval(fa, new_page->KeyAt(0), new_page_id_, ctx);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInterval(InternalPage* cur, const KeyType &key, const page_id_t value, Context &ctx) {
  int n = cur->GetSize();
  if (n < cur->GetMaxSize()) {
    cur->SetSize(n + 1);
    bool flag = true;
    for (int i = n; i > 1; i--) {
      if (comparator_(cur->KeyAt(i - 1), key) < 0) {
        flag = false;
        cur->SetKeyAt(i, key);
        cur->SetValueAt(i, value);
        break;
      } else {
        cur->SetKeyAt(i, cur->KeyAt(i - 1));
        cur->SetValueAt(i, cur->ValueAt(i - 1));
      }
    }
    if (flag) {
      cur->SetKeyAt(1, key);
      cur->SetValueAt(1, value);
    }
    return;
  }

  auto new_page_id_ = bpm_->NewPage();
  auto new_page_guard = bpm_->WritePage(new_page_id_);
  auto new_page = new_page_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_);

  std::vector<KeyType> v(n + 1);
  std::vector<page_id_t> v2(n + 1);
  v2[0] = cur->ValueAt(0);
  for (int i = 1; i < n; i++) {
    v[i] = cur->KeyAt(i);
    v2[i] = cur->ValueAt(i);
  }
  bool flag = true;
  for (int i = n; i > 1; i--) {
    if (comparator_(v[i - 1], key) > 0) {
      v[i] = v[i - 1];
      v2[i] = v2[i - 1];
    } else {
      v[i] = key;
      v2[i] = value;
      flag = false;
      break;
    }
  }
  if (flag) {
    v[1] = key;
    v2[1] = value;
  }
  int mid = (n + 1) / 2;
  cur->SetSize(mid);
  for (int i = 1; i < mid; i ++ ) {
    cur->SetKeyAt(i, v[i]);
    cur->SetValueAt(i, v2[i]);
  }

  new_page->SetSize(v.size() - mid);
  for (size_t i = mid + 1; i < v.size(); i++) {
    new_page->SetKeyAt(i - mid, v[i]);
    new_page->SetValueAt(i - mid, v2[i]);
  }
  new_page->SetValueAt(0, v2[mid]);


  if (ctx.IsRootPage(ctx.page_id_)) {
    auto new_page_id_2 = bpm_->NewPage();
    auto new_page_guard2 = bpm_->WritePage(new_page_id_2);
    auto new_page2 = new_page_guard2.AsMut<InternalPage>();
    new_page2->Init(internal_max_size_);
    new_page2->SetSize(2);
    new_page2->SetKeyAt(1, v[mid]);
    new_page2->SetValueAt(1, new_page_id_);
    new_page2->SetValueAt(0, ctx.page_id_);
    auto root_page_pre = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    root_page_pre->root_page_id_ = new_page_id_2;
  } else {
    auto bk_gard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto fa = bk_gard.AsMut<InternalPage>();
    ctx.page_id_ = bk_gard.GetPageId();
    //InsertInterval(cur, key, value, ctx);
    InsertInterval(fa, v[mid], new_page_id_, ctx);
  }


}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page_pre = guard.As<BPlusTreeHeaderPage>();
  return root_page_pre->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
