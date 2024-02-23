from recostNode import *
from util import *
import math

class Cost:
    def __init__(self):
        self.startup = None
        self.per_tuple = None

# 计算给定约束条件下的代价
def cost_qual_eval(clauses, root: DerivedPlanNode):
    cost_estim = Cost()
    for qual in clauses:
        cost_qual_eval_walker(qual, root, cost_estim)
    return cost_estim
# TODO: 计算给定的约束条件下的代价,并记录在参数cost中
def cost_qual_eval_walker(clause, root, cost):
    # 需要对不同的clause类型进行计算
    # 这里原本的实现用到了很多指针的特性,代码复杂度较大
    pass

def cost_rescan(root: DerivedPlanNode):
    node_type = root.node_type
    startup_cost, total_cost = 0, 0
    if node_type == "Function Scan":
        startup_cost = 0
        total_cost = root.total_cost - root.startup_cost
    elif node_type == "Hash Join":
        if root.numbatches == 1:
            startup_cost = 0
            total_cost = root.total_cost- root.startup_cost
        else:
            startup_cost = root.startup_cost
            total_cost = root.total_cost
    elif node_type == "CTE Scan" or node_type == "WorkTableScan":
        run_cost = DefaultOSCost.CPU_TUPLE_COST.value * root.rows
        nbytes = relation_byte_size(root.rows, root.width)
        if nbytes > DefaultOSSize.WORK_MEM.value * 1024:
            run_cost += DefaultOSCost.SEQ_PAGE_COST.value * math.ceil(nbytes / DefaultOSSize.BLCKSZ.value)
        startup_cost = 0
        total_cost = run_cost
    elif node_type == "Materialize" or node_type == "SortNode":
        run_cost = DefaultOSCost.CPU_OPERATOR_COST.value * root.rows
        nbytes = relation_byte_size(root.rows, root.width)
        if nbytes > DefaultOSSize.WORK_MEM.value * 1024:
            run_cost += DefaultOSCost.SEQ_PAGE_COST.value * math.ceil(nbytes / DefaultOSSize.BLCKSZ.value)
        startup_cost = 0
        total_cost = run_cost
    else:
        startup_cost = root.startup_cost
        total_cost = root.total_cost
    return startup_cost, total_cost

#// # TODO: 判断当前路径是否支持mark或者restore
#// # TODO: 未完成
#// def ExecSupportsMarkRestore(root):
#//     pathtype = type(root)
#//     # * 这里需要其他节点类型的名字接口,后续可能需要进行修改
#//     if pathtype == IndexScanNode or \
#//         pathtype == IndexOnlyScanNode or \
#//         pathtype == MaterialNode or \
#//         pathtype == SortNode:
#//         return True
#//     # ? 自定义扫描不是很理解
#//     # TODO: 暂时假设其不存在
#//     # elif pathtype == CustomScanNode:
#//     #     pass
#//     # ? Result节点不是很理解
#//     # TODO: 暂时假设其不存在
#//     # elif pathtype == ResultNode:
#//     #     pass
#//   
#//     return False


