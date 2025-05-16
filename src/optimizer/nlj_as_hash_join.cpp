#include <execution/expressions/logic_expression.h>

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void ParseAndExpression(const AbstractExpressionRef& predicate,
                        std::vector<AbstractExpressionRef>* left_key_expressions,
                        std::vector<AbstractExpressionRef>* right_key_expressions){
  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(predicate);
  if(logic_expr){
    // 如果可以转换为逻辑表达式，说明有多个表达式，继续分解
    ParseAndExpression(logic_expr->GetChildAt(0),left_key_expressions,right_key_expressions);
    ParseAndExpression(logic_expr->GetChildAt(1),left_key_expressions,right_key_expressions);
  }
  // 如果此时不是逻辑表达式，看看是不是比较表达式
  auto comparision_expr = std::dynamic_pointer_cast<const ComparisonExpression>(predicate);
  if(comparision_expr){
    // 如果是逻辑表达式，看看是不是等值表达式
    auto com_type = comparision_expr->comp_type_;
    // 只能是等值表达式
    if(com_type == ComparisonType::Equal){
        // 拿到左 column value
        auto column_value_1 = std::dynamic_pointer_cast<const ColumnValueExpression>(comparision_expr->GetChildAt(0));
        if(column_value_1->GetTupleIdx() == 0){
            // tuple index 0 = left side of join, tuple index 1 = right side of join
            left_key_expressions->emplace_back(comparision_expr->GetChildAt(0));
            right_key_expressions->emplace_back(comparision_expr->GetChildAt(1));
        }else{
            left_key_expressions->emplace_back(comparision_expr->GetChildAt(1));
            right_key_expressions->emplace_back(comparision_expr->GetChildAt(0));
        }
    }
  }
}


auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));
  if(optimized_plan->GetType() == PlanType::NestedLoopJoin){
    const auto&join_plan = dynamic_cast<const NestedLoopJoinPlanNode&>(*optimized_plan);
    // 拿到谓词
    auto predicate = join_plan.Predicate();
    // 查看改谓词两侧是不是等值表达式，解析表达式
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    ParseAndExpression(predicate, &left_key_expressions, &right_key_expressions);
    return std::make_shared<HashJoinPlanNode>(join_plan.output_schema_, join_plan.GetLeftPlan(),
                                            join_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                            join_plan.GetJoinType());
  }
  return optimized_plan;
}


}  // namespace bustub
