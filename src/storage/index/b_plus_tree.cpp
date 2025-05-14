#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"
// 这行注释测试git push
namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id)
       {
  bplus_latch_ = std::make_shared<std::mutex>();
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
   // std::cout << "in循环:" << std::endl;
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
    ctx.cur_page_ = std::move(ctx.write_set_.back());
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
  //std::unique_lock lock(*bplus_latch_);
  bplus_latch_->lock();
  if (IsEmpty()) {
    bplus_latch_->unlock();
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
      bplus_latch_->unlock();
      return true;
    }
  }
  bplus_latch_->unlock();
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
  //std::unique_lock latch(*bplus_latch_);
  bplus_latch_->lock();
  Context ctx;
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  ctx.header_page_ = std::move(guard);
  auto root_page_pre = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  //当前树为空
  if (root_page_pre->root_page_id_ == INVALID_PAGE_ID) {
    root_page_pre->root_page_id_ = bpm_->NewPage();
    WritePageGuard root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
    auto ans = root_guard.AsMut<LeafPage>();
    ans->Init(leaf_max_size_);
    ans->ChangeSizeBy(1);
    ans->SetKeyAt(0, key);
    ans->SetValueAt(0, value);
    bplus_latch_->unlock();
    return true;
  }

  //初始化
  WritePageGuard root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
  auto res = root_guard.AsMut<BPlusTreePage>();
  //CtxInit(ctx, std::move(guard));
  ctx.root_page_id_ = root_page_pre->root_page_id_;
  ctx.page_id_ = root_page_pre->root_page_id_;
  ctx.write_set_.push_back(std::move(root_guard));

  //std::cout << "insert拿叶子节点:" << std::endl;
  auto leaf_page = GetLeafPage(res, ctx, key);
  //std::cout << "insert拿到了:" << std::endl;
  auto cur = reinterpret_cast<LeafPage*>(leaf_page);
  for (int i = 0; i < cur->GetSize(); i++) {
    if (comparator_(cur->KeyAt(i), key) == 0) {
      bplus_latch_->unlock();
      return false;
    }
  }
  //当前叶子空间足够，不需要分裂
  //std::cout << "insert拿叶子节点:" << std::endl;
  if (cur->GetSize() < cur->GetMaxSize()) {
    int i = 0;
    while (comparator_(key, cur->KeyAt(i)) > 0 && i < cur->GetSize()) i++;
    for (int j = cur->GetSize(); j > i; j--) {
      cur->SetKeyAt(j, cur->KeyAt(j - 1));
      cur->SetValueAt(j, cur->ValueAt(j - 1));
    }
    cur->SetKeyAt(i, key);
    cur->SetValueAt(i, value);
    cur->SetSize(cur->GetSize() + 1);
    bplus_latch_->unlock();
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
  int i = 0, j;
  while (comparator_(key, v[i]) > 0 && i < cur->GetSize()) i++;
  for (int j = cur->GetSize(); j > i; j--) {
    v[j] = v[j - 1];
    v2[j] = v2[j - 1];
  }
  v[i] = key;
  v2[i] = value;
  int max_size = cur->GetSize();
  cur->SetSize((max_size + 1) / 2);
  new_page->SetSize(max_size + 1 - (max_size + 1) / 2);
  new_page->SetNextPageId(cur->GetNextPageId());
  cur->SetNextPageId(new_page_id_);


  for (i = 0; i < cur->GetSize(); i++) {
    cur->SetKeyAt(i, v[i]);
    cur->SetValueAt(i, v2[i]);
  }
  for (i = 0, j = cur->GetSize(); i < new_page->GetSize(); i++, j++) {
    new_page->SetKeyAt(i, v[j]);
    new_page->SetValueAt(i, v2[j]);
  }

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
    ctx.cur_page_ = std::move(bk_gard);
    //InsertInterval(cur, key, value, ctx);
    InsertInterval(fa, new_page->KeyAt(0), new_page_id_, ctx);
  }
  bplus_latch_->unlock();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInterval(InternalPage* cur, const KeyType &key, const page_id_t value, Context &ctx) {
  if (cur->GetSize() < cur->GetMaxSize()) {
    int i = 1;
    while (comparator_(key, cur->KeyAt(i)) > 0 && i < cur->GetSize()) i++;
    for (int j = cur->GetSize(); j > i; j--) {
      cur->SetKeyAt(j, cur->KeyAt(j - 1));
      cur->SetValueAt(j, cur->ValueAt(j - 1));
    }
    cur->SetKeyAt(i, key);
    cur->SetValueAt(i, value);
    cur->SetSize(cur->GetSize() + 1);
  } else {
    auto new_page_id_ = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page_id_);
    auto new_page = new_page_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);

    std::vector<KeyType> v(cur->GetSize() + 1);
    std::vector<page_id_t> v2(cur->GetSize() + 1);
    v2[0] = cur->ValueAt(0);
    for (int i = 1; i < cur->GetSize(); i++) {
      v[i] = cur->KeyAt(i);
      v2[i] = cur->ValueAt(i);
    }
    int i = 1, j;
    while (comparator_(key, v[i]) > 0 && i < cur->GetSize()) i++;
    for (j = cur->GetSize(); j > i; j--) {
      v[j] = v[j - 1];
      v2[j] = v2[j - 1];
    }
    v[i] = key;
    v2[i] = value;
    int mid = (cur->GetSize() + 2) / 2;
    cur->SetSize(mid);
    for (i = 1; i < mid; i ++ ) {
      cur->SetKeyAt(i, v[i]);
      cur->SetValueAt(i, v2[i]);
    }

    new_page->SetSize(v.size() - mid);
    for (i = mid + 1; i < (int)v.size(); i++) {
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
      ctx.cur_page_ = std::move(bk_gard);
      //InsertInterval(cur, key, value, ctx);
      InsertInterval(fa, v[mid], new_page_id_, ctx);
    }
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
  //std::unique_lock lock(*bplus_latch_);
  bplus_latch_->lock();
  Context ctx;
  auto guard = bpm_->WritePage(header_page_id_);
  auto root_page_pre = guard.AsMut<BPlusTreeHeaderPage>();
  // 如果当前树空，直接返回
  if (root_page_pre->root_page_id_ == INVALID_PAGE_ID) {
    bplus_latch_->unlock();
    return;
  }
  ctx.root_page_id_ = root_page_pre->root_page_id_;
  ctx.page_id_ = root_page_pre->root_page_id_;
  ctx.header_page_ = std::move(guard);
  auto root_guard = bpm_->WritePage(root_page_pre->root_page_id_);
  auto root_page = root_guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(root_guard));
 // std::cout << "de拿叶子节点:" << std::endl;
  auto cnmm = root_page;
  while (!cnmm->IsLeafPage()) {
    //std::cout << "de循环:" << std::endl;
    auto curr = reinterpret_cast<InternalPage*>(cnmm);
    int i = 1;
    while (i < curr->GetSize()) {
      if (comparator_(curr->KeyAt(i), key) > 0) {
        auto ans = curr->ValueAt(i - 1);
        WritePageGuard wgg = bpm_->WritePage(ans);
        cnmm = wgg.AsMut<BPlusTreePage>();
        ctx.write_set_.push_back(std::move(wgg));
        ctx.page_id_ = ans;
        break;
      }
      i++;
    }
    if (i == curr->GetSize()) {
      auto ans = curr->ValueAt(i - 1);
      WritePageGuard wgg = bpm_->WritePage(ans);
      cnmm = wgg.AsMut<BPlusTreePage>();
      ctx.write_set_.push_back(std::move(wgg));
      ctx.page_id_ = ans;
    }
  }
  if (ctx.write_set_.size() > 1) {
    ctx.cur_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
  }
  auto cnm = cnmm;
  //std::cout << "de拿到了:" << std::endl;
  //ctx.cur_page_ = std::nullopt;
  auto leaf_page = reinterpret_cast<LeafPage*>(cnm);
  bool found = false;
  int pos;
  for (pos = 0; pos < leaf_page->GetSize(); pos++) {
    if (comparator_(leaf_page->KeyAt(pos), key) == 0) {
      found = true;
      break;
    }
  }
  //如果当前未找到，直接返回
  if (!found) {
    bplus_latch_->unlock();
    return;
  }
  for (int i = pos; i < leaf_page->GetSize() - 1; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  leaf_page->SetSize(leaf_page->GetSize() - 1);
  //如果当前删的是根节点，在size为0时，需要设置树为空
  if (ctx.IsRootPage(ctx.page_id_)) {
    if (leaf_page->GetSize() == 0) {
      root_page_pre->root_page_id_ = INVALID_PAGE_ID;
    }
    bplus_latch_->unlock();
    return;
  }
  //如果当前size>=minsize,直接返回
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    bplus_latch_->unlock();
    return;
  }
  auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  page_id_t parent_page_id = ctx.write_set_.back().GetPageId();
  int leftSibling = -1, rightSibling = - 1;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    if (ctx.page_id_ == parent_page->ValueAt(i)) {
      leftSibling = i - 1;
      rightSibling = i + 1;
    }
  }
  auto cur = leaf_page;
  //向左兄弟借一个
  if (leftSibling >= 0) {
    page_id_t left_page_id = parent_page->ValueAt(leftSibling);
    auto left_page_guard = bpm_->WritePage(left_page_id);
    auto left_page = left_page_guard.AsMut<LeafPage>();
    if (left_page->GetSize() > left_page->GetMinSize()) {
      for (int i = cur->GetSize(); i > 0; i--) {
        cur->SetKeyAt(i, cur->KeyAt(i - 1));
        cur->SetValueAt(i, cur->ValueAt(i - 1));
      }
      cur->SetSize(cur->GetSize() + 1);
      cur->SetKeyAt(0, left_page->KeyAt(left_page->GetSize() - 1));
      cur->SetValueAt(0, left_page->ValueAt(left_page->GetSize() - 1));
      left_page->SetSize(left_page->GetSize() - 1);
      parent_page->SetKeyAt(leftSibling + 1, cur->KeyAt(0));
      bplus_latch_->unlock();
      return;
    }
  }
  //向右兄弟借一个
  if (rightSibling < parent_page->GetSize()) {
    page_id_t right_page_id = parent_page->ValueAt(rightSibling);
    auto right_page_guard = bpm_->WritePage(right_page_id);
    auto right_page = right_page_guard.AsMut<LeafPage>();

    if (right_page->GetSize() > right_page->GetMinSize()) {
      cur->SetSize(cur->GetSize() + 1);
      cur->SetKeyAt(cur->GetSize() - 1, right_page->KeyAt(0));
      cur->SetValueAt(cur->GetSize() - 1, right_page->ValueAt(0));
      right_page->SetSize(right_page->GetSize() - 1);
      for (int i = 0; i < right_page->GetSize(); i++) {
        right_page->SetKeyAt(i, right_page->KeyAt(i + 1));
        right_page->SetValueAt(i, right_page->ValueAt(i + 1));
      }
      parent_page->SetKeyAt(rightSibling, right_page->KeyAt(0));
      bplus_latch_->unlock();
      return;
    }
  }
  // 左右兄弟都不够借，只能合并
  if (leftSibling >= 0) {
    page_id_t left_page_id = parent_page->ValueAt(leftSibling);
    auto left_page_guard = bpm_->WritePage(left_page_id);
    auto left_page = left_page_guard.AsMut<LeafPage>();

    for (int i = left_page->GetSize(), j = 0; j < cur->GetSize(); i++, j++) {
      left_page->SetKeyAt(i, cur->KeyAt(j));
      left_page->SetValueAt(i, cur->ValueAt(j));
    }
    left_page->SetSize(left_page->GetSize() + cur->GetSize());
    left_page->SetNextPageId(cur->GetNextPageId());

    ctx.cur_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    page_id_t cnmm = ctx.page_id_;
    ctx.page_id_ = parent_page_id;

    removeInternal(parent_page->KeyAt(leftSibling + 1), parent_page, ctx, cnmm);

  } else if (rightSibling < parent_page->GetSize()) {
    page_id_t right_page_id = parent_page->ValueAt(rightSibling);
    auto right_page_guard = bpm_->WritePage(right_page_id);
    auto right_page = right_page_guard.AsMut<LeafPage>();

    for (int i = cur->GetSize(), j = 0; j < right_page->GetSize(); i++, j++) {
      cur->SetKeyAt(i, right_page->KeyAt(j));
      cur->SetValueAt(i, right_page->ValueAt(j));
    }
    cur->SetSize(cur->GetSize() + right_page->GetSize());
    cur->SetNextPageId(right_page->GetNextPageId());

    ctx.cur_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    ctx.page_id_ = parent_page_id;
    removeInternal(parent_page->KeyAt(rightSibling), parent_page, ctx, right_page_id);

  }
  bplus_latch_->unlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::removeInternal(const KeyType &key, InternalPage* cur, Context &ctx, page_id_t child) {
  if (ctx.IsRootPage(ctx.page_id_)) {
    if (cur->GetSize() == 2) {
      if (cur->ValueAt(0) == child) {
        auto root_page_pre = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        root_page_pre->root_page_id_ = cur->ValueAt(1);
        return;
      } else if (cur->ValueAt(1) == child) {
        auto root_page_pre = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
        root_page_pre->root_page_id_ = cur->ValueAt(0);
        return;
      }
    }
  }
  int pos;
  for (pos = 1; pos < cur->GetSize(); pos++) {
    if (comparator_(cur->KeyAt(pos), key) == 0) {
      break;
    }
  }

  for (int i = pos; i < cur->GetSize() - 1; i++) {
    cur->SetKeyAt(i, cur->KeyAt(i + 1));
  }
  // for (pos = 0; pos < cur->GetSize(); pos++) {
  //   if (cur->ValueAt(pos) == child) {
  //     break;
  //   }
  // }
  for (int i = pos; i < cur->GetSize() - 1; i++) {
    cur->SetValueAt(i, cur->ValueAt(i + 1));
  }
  cur->SetSize(cur->GetSize() - 1);

  if (cur->GetSize() >= cur->GetMinSize()) {
    return;
  }
  if (ctx.IsRootPage(ctx.page_id_)) {
    return;
  }
  auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  page_id_t parent_page_id = ctx.write_set_.back().GetPageId();
  int leftSibling = -1;
  int rightSibling = -1;
  for (pos = 0; pos < parent_page->GetSize(); pos++) {
    if (parent_page->ValueAt(pos) == ctx.page_id_) {
      leftSibling = pos - 1;
      rightSibling = pos + 1;
      break;
    }
  }
  //向兄弟借TODO
  if (leftSibling >= 0) {
    page_id_t left_page_id = parent_page->ValueAt(leftSibling);
    auto left_page_guard = bpm_->WritePage(left_page_id);
    auto left_page = left_page_guard.AsMut<InternalPage>();
    if (left_page->GetSize() > left_page->GetMinSize()) {
      for (int i = cur->GetSize(); i > 1; i--) {
        cur->SetKeyAt(i, cur->KeyAt(i - 1));
      }
      cur->SetKeyAt(1, parent_page->KeyAt(leftSibling + 1));
      parent_page->SetKeyAt(leftSibling + 1, left_page->KeyAt(left_page->GetSize() - 1));

      for (int i = cur->GetSize(); i > 0; i--) {
        cur->SetValueAt(i, cur->ValueAt(i - 1));
      }
      cur->SetValueAt(0, left_page->ValueAt(left_page->GetSize() - 1));
      cur->SetSize(cur->GetSize() + 1);
      left_page->SetSize(left_page->GetSize() - 1);
      return;
    }
  }
  if (rightSibling < parent_page->GetSize()) {
    page_id_t right_page_id = parent_page->ValueAt(rightSibling);
    auto right_page_guard = bpm_->WritePage(right_page_id);
    auto right_page = right_page_guard.AsMut<InternalPage>();

    if (right_page->GetSize() > right_page->GetMinSize()) {
      cur->SetKeyAt(cur->GetSize(), parent_page->KeyAt(rightSibling));
      parent_page->SetKeyAt(rightSibling, right_page->KeyAt(1));

      for (int i = 1; i < right_page->GetSize() - 1; i++) {
        right_page->SetKeyAt(i, right_page->KeyAt(i + 1));
      }
      cur->SetValueAt(cur->GetSize(), right_page->ValueAt(0));

      for (int i = 0; i < right_page->GetSize() - 1; ++i) {
        right_page->SetValueAt(i, right_page->ValueAt(i + 1));
      }
      cur->SetSize(cur->GetSize() + 1);
      right_page->SetSize(right_page->GetSize() - 1);
      return;
    }
  }
  //兄弟不够借用，合并TODO
  if (leftSibling >= 0) {
    page_id_t left_page_id = parent_page->ValueAt(leftSibling);
    auto left_page_guard = bpm_->WritePage(left_page_id);
    auto left_page = left_page_guard.AsMut<InternalPage>();
    left_page->SetKeyAt(left_page->GetSize(), parent_page->KeyAt(leftSibling + 1));

    for (int i = left_page->GetSize() + 1, j = 1; j < cur->GetSize(); j++) {
      left_page->SetKeyAt(i, cur->KeyAt(j));
    }
    for (int i = left_page->GetSize(), j = 0; j < cur->GetSize(); j++) {
      left_page->SetValueAt(i, cur->ValueAt(j));
    }
    left_page->SetSize(left_page->GetSize() + cur->GetSize());
    cur->SetSize(0);

    ctx.cur_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    page_id_t cnmm = ctx.page_id_;
    ctx.page_id_ = parent_page_id;
    removeInternal(parent_page->KeyAt(leftSibling + 1), parent_page, ctx, cnmm);

  } else if (rightSibling < parent_page->GetSize()) {
    page_id_t right_page_id = parent_page->ValueAt(rightSibling);
    auto right_page_guard = bpm_->WritePage(right_page_id);
    auto right_page = right_page_guard.AsMut<InternalPage>();
    cur->SetKeyAt(cur->GetSize(), parent_page->KeyAt(rightSibling));

    for (int i = cur->GetSize() + 1, j = 1; j < right_page->GetSize(); i++, j++) {
      cur->SetKeyAt(i, right_page->KeyAt(j));
    }
    for (int i = cur->GetSize(), j = 0; j < right_page->GetSize(); i++, j++) {
      cur->SetValueAt(i, right_page->ValueAt(j));
    }
    cur->SetSize(cur->GetSize() + right_page->GetSize());
    right_page->SetSize(0);

    ctx.cur_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    ctx.page_id_ = parent_page_id;
    removeInternal(parent_page->KeyAt(rightSibling), parent_page, ctx, right_page_id);

  }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  bplus_latch_->lock();
  //std::unique_lock lock(*bplus_latch_);
  auto guard = bpm_->WritePage(header_page_id_);
  auto res = guard.As<BPlusTreeHeaderPage>();
  if (res->root_page_id_ == INVALID_PAGE_ID) {
    bplus_latch_->unlock();
    return INDEXITERATOR_TYPE(bpm_, -1, 0, true);
  }
  auto root_guard = bpm_->WritePage(res->root_page_id_);
  auto ans = root_guard.As<BPlusTreePage>();
  if (ans->IsLeafPage()) {
    bplus_latch_->unlock();
    return INDEXITERATOR_TYPE(bpm_, res->root_page_id_, 0);
  }
  auto cur = root_guard.As<InternalPage>();

  while (!cur->IsLeafPage()) {
    page_id_t page_id = cur->ValueAt(0);
    auto wg = bpm_->WritePage(page_id);
    auto res = wg.AsMut<BPlusTreePage>();
    if (res->IsLeafPage()) {

      bplus_latch_->unlock();
      return INDEXITERATOR_TYPE(bpm_, page_id, 0);
    }
    cur = wg.AsMut<InternalPage>();
  }
  bplus_latch_->unlock();
  return INDEXITERATOR_TYPE(bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  //std::unique_lock lock(*bplus_latch_);
  bplus_latch_->lock();
  auto guard = bpm_->WritePage(header_page_id_);
  auto ans = guard.As<BPlusTreeHeaderPage>();
  if (ans->root_page_id_ == INVALID_PAGE_ID) {
    bplus_latch_->unlock();
    return INDEXITERATOR_TYPE(bpm_, -1, 0, true);
  }
  auto root_guard = bpm_->WritePage(ans->root_page_id_);
  auto res = root_guard.AsMut<BPlusTreePage>();
  page_id_t page_id = -1;
  while (!res->IsLeafPage()) {
    auto cur = reinterpret_cast<InternalPage*>(res);
    int i = 1;
    while (i < cur->GetSize()) {
      if (comparator_(cur->KeyAt(i), key) > 0) {
        auto ans = cur->ValueAt(i - 1);
        WritePageGuard wg = bpm_->WritePage(ans);
        res = wg.AsMut<BPlusTreePage>();
        page_id = wg.GetPageId();
        break;
      }
      i++;
    }
    if (i == cur->GetSize()) {
      auto ans = cur->ValueAt(i - 1);
      WritePageGuard wg = bpm_->WritePage(ans);
      res = wg.AsMut<BPlusTreePage>();
      page_id = wg.GetPageId();
    }
  }
  auto cur = reinterpret_cast<LeafPage*>(res);
   for (int i = 0; i < cur->GetSize(); i++) {
    if (comparator_(cur->KeyAt(i), key) == 0) {
      bplus_latch_->unlock();
      return INDEXITERATOR_TYPE(bpm_, page_id, i);
    }
  }
  bplus_latch_->unlock();
  throw std::runtime_error("unimplemented");
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_, -1, 0, true);
}

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
