from Recost_tmp.util import *
from Recost_tmp.PlanNode.planNodeAPI import *
from Recost_tmp.PlanNode.filterNode import Cost, Restrict, PredicateNode, Operator
import math

def get_operator_cost(op: Operator):
    # <> = < <= > >=运算符都是1
    if op == Operator.NEQ or op == Operator.EQ or\
        op == Operator.LEQ or op == Operator.GEQ or\
        op == Operator.IS_DISTINCT_FROM:
        return 1
    # like都是1
    if op == Operator.LIKE or op == Operator.NOT_LIKE:
        return 1
    # BooleanTest NullTest都是1（似乎在源码中代价是0来着
    if op == Operator.IS_NOT_NULL or op == Operator.IS_NULL or\
        op == Operator.IS_TRUE or op == Operator.IS_NOT_TRUE or\
        op == Operator.IS_FALSE or op == Operator.IS_NOT_FALSE or\
        op == Operator.IS_UNKNOWN or op == Operator.IS_NOT_UNKNOWN:
        return 1
    
    if op == Operator.PLUS or op == Operator.MINUS or \
        op == Operator.MULTI or op == Operator.DIVI or op == Operator.MOD:
        return 1
    
    if op == Operator.COERCE:
        return 1
    # 只识别一般的操作符, 对于IN子句, 另作判断
    # IN
    # if op == Operator.IN:
    #     # 假设在一半的时候会判定成功
    #     return 1 * 
    #     pass
    assert False, "illegal Operator"
    


# 计算给定约束条件下的代价
def cost_qual_eval(clauses: List[PredicateNode], root: PlanNodeInterface):
    cost_estim = Cost()
    for qual in clauses:
        cost_qual_eval_walker(qual, root, cost_estim)
    return cost_estim

# TODO: 计算给定的约束条件下的代价,并记录在参数cost中
def cost_qual_eval_walker(clause: PredicateNode, root, cost: Cost):
    # 需要对不同的clause类型进行计算
    # 这里原本的实现用到了很多指针的特性,代码复杂度较大
    
    # 目前只实现PredicateNode中可以表达的表达式的代价
    # 类型转化的代价基本都是一次CPU计算的代价，这里为方便计算, 因此忽略不计类型转化的代价
    # 同样忽略_p函数的代价
    if clause.type == Restrict.Const:
        pass
    elif clause.type == Restrict.Var:
        # 排除列属性
        if len(clause.children) != 0:
            cost.startup += get_operator_cost(clause.operator) * DefaultOSCost.CPU_OPERATOR_COST
    elif clause.type == Restrict.AndClause or \
        clause.type == Restrict.OrClause or \
        clause.type == Restrict.NotClause:
        pass
    elif clause.type == Restrict.OpExpr or \
        clause.type == Restrict.DistinctExpr:
        # clause.type == Restrict.NullIfExpr or \  # 不是很懂, 暂时忽略
        cost.per_tuple += get_operator_cost(clause.operator) * DefaultOSCost.CPU_OPERATOR_COST
    elif clause.type == Restrict.ScalarArrayOpExpr:
        # 约定右边的始终为常数数组
        cost.per_tuple += get_operator_cost(Operator.EQ) * len(clause.children[1].text) * DefaultOSCost.CPU_OPERATOR_COST * 0.5
    elif clause.type == Restrict.NullTest or clause.type == Restrict.BooleanTest:
        cost.per_tuple += get_operator_cost(clause.operator) * DefaultOSCost.CPU_OPERATOR_COST
    else: # 其他暂不考虑
        assert False, "error qual walker"

    for children in clause.children:
        son_const = Cost()
        cost_qual_eval_walker(children, root, son_const)
        cost.startup += son_const.startup
        cost.per_tuple += son_const.per_tuple

def cost_rescan(root: PlanNodeInterface):
    node_type = root.node_type
    startup_cost, total_cost = 0, 0
    if node_type == NodeType.FUNCTION_SCAN:
        startup_cost = 0
        total_cost = root.total_cost - root.startup_cost
    elif node_type == NodeType.HASH_JOIN:
        if root.numbatches == 1:
            startup_cost = 0
            total_cost = root.total_cost- root.startup_cost
        else:
            startup_cost = root.startup_cost
            total_cost = root.total_cost
    elif node_type == NodeType.CTE_SCAN or node_type == NodeType.WORK_TABLE_SCAN:
        run_cost = DefaultOSCost.CPU_TUPLE_COST * root.est_rows
        nbytes = relation_byte_size(root.est_rows, root.width)
        if nbytes > DefaultOSSize.WORK_MEM * 1024:
            run_cost += DefaultOSCost.SEQ_PAGE_COST * math.ceil(nbytes / DefaultOSSize.BLCKSZ)
        startup_cost = 0
        total_cost = run_cost
    elif node_type == NodeType.MATERIALIZE or node_type == NodeType.SORT:
        run_cost = DefaultOSCost.CPU_OPERATOR_COST * root.est_rows
        nbytes = relation_byte_size(root.est_rows, root.width)
        if nbytes > DefaultOSSize.WORK_MEM * 1024:
            run_cost += DefaultOSCost.SEQ_PAGE_COST * math.ceil(nbytes / DefaultOSSize.BLCKSZ)
        startup_cost = 0
        total_cost = run_cost
    else:
        startup_cost = root.startup_cost
        total_cost = root.total_cost
    return startup_cost, total_cost


