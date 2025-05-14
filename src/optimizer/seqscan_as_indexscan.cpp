
#include <execution/expressions/column_value_expression.h>
#include <execution/expressions/comparison_expression.h>
#include <execution/expressions/constant_value_expression.h>
#include <execution/expressions/logic_expression.h>
#include <execution/plans/index_scan_plan.h>
#include <execution/plans/seq_scan_plan.h>

#include "optimizer/optimizer.h"

namespace bustub {
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    //将当前scan转化为seqscan
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode&>(*optimized_plan);
    //如果等于null，不需要索引
    if (seq_plan.filter_predicate_ != nullptr) {
      auto table_info = catalog_.GetTable(seq_plan.table_oid_);
      auto table_indexs = catalog_.GetTableIndexes(seq_plan.table_name_);

      // 如果filter_pre是一个逻辑谓词 例如v1 = 1 AND v2 = 2, 则直接返回, 因为bustub的索引都是单列的, 两者不可能匹配
      auto tmp = dynamic_cast<LogicExpression *>(seq_plan.filter_predicate_.get());
      if (tmp != nullptr && tmp->logic_type_ == LogicType::Or) {
        std::deque<AbstractExpression*>cnm_mmp;
        cnm_mmp.push_back(tmp);
        std::vector<AbstractExpression*>jisuan;
        while (!cnm_mmp.empty()) {
          auto tt = cnm_mmp.front();
          cnm_mmp.pop_front();
          auto cnmleft = dynamic_cast<LogicExpression *>(tt->children_[0].get());
          auto cnmright = dynamic_cast<LogicExpression *>(tt->children_[1].get());
          if (cnmleft != nullptr) {
            cnm_mmp.push_back(cnmleft);
          } else {
            jisuan.push_back(tt->children_[0].get());
          }
          if (cnmright != nullptr) {
            cnm_mmp.push_back(cnmright);
          } else {
            jisuan.push_back(tt->children_[1].get());
          }
        }
        //std::cout << "cnmmm:" << jisuan.size() << std::endl;
        //std::cout <<"cnm:" <<(tmp->logic_type_ == LogicType::Or) << ' ' << tmp->children_.size() << std::endl;
        std::vector<uint32_t> res[jisuan.size()];
        int cnt = 0;
        std::vector<AbstractExpressionRef>ans;
        for (auto chi : jisuan) {
          auto left = dynamic_cast<ColumnValueExpression *>(chi->children_[0].get());
          auto right = dynamic_cast<ColumnValueExpression *>(chi->children_[1].get());
          if (left != nullptr) {
            res[cnt].push_back(left->GetColIdx());
            ans.push_back(chi->children_[1]);
          }
          if (right != nullptr) {
            res[cnt].push_back(right->GetColIdx());
            ans.push_back(chi->children_[0]);
          }
          cnt++;
        }
        bool f = true;
        for (int i = 0; i < cnt - 1; i ++ ) {
          if (res[i] != res[i + 1]) {
            f = false;
          }
        }
        //std::cout << "woc:" << f << std::endl;
        for (auto index : table_indexs) {
          if (f && res[0] == index->index_->GetKeyAttrs()){
            //std::cout << "woc:" << ans.size() << std::endl;
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                     index->index_oid_, seq_plan.filter_predicate_, ans);
          }
        }

      }
      if (tmp != nullptr || table_indexs.empty()) {
        return optimized_plan;
      }
      // 这个filter_pri实际上是一个compExpr 例如where v1=1
      auto filter_comp_expr = dynamic_cast<ComparisonExpression *>(seq_plan.filter_predicate_.get());
      if (filter_comp_expr != nullptr && filter_comp_expr->comp_type_ == ComparisonType::Equal) {
        //提取列
        auto left = dynamic_cast<ColumnValueExpression *>(filter_comp_expr->children_[0].get());
        auto right = dynamic_cast<ColumnValueExpression *>(filter_comp_expr->children_[1].get());
        std::vector<uint32_t> res;
        if (left != nullptr && right == nullptr) {
          res.push_back(left->GetColIdx());
          for (auto index : table_indexs) {
            if (index->index_->GetKeyAttrs() == res) {
              std::vector<AbstractExpressionRef>ans;
              ans.push_back(filter_comp_expr->children_[1]);
              return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                       index->index_oid_, seq_plan.filter_predicate_, ans);
            }
          }
        }
        else if (left == nullptr && right != nullptr) {
          res.push_back(right->GetColIdx());
          for (auto index : table_indexs) {
            if (index->index_->GetKeyAttrs() == res) {
              std::vector<AbstractExpressionRef>ans;
              ans.push_back(filter_comp_expr->children_[0]);
              return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                       index->index_oid_, seq_plan.filter_predicate_, ans);
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}


}// namespace bustub