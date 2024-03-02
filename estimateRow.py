from Recost_tmp.util import *
from Recost_tmp.utilsel import *
from Recost_tmp.PlanNode.filterNode import PredicateNode, Restrict, Operator
from Recost_tmp.PlanNode.planNodeAPI import *


def calc_joinrel_size_estimate(root: JoinNodeInterface, left_node: PlanNodeInterface, right_node: PlanNodeInterface):
    join_type = root.join_type
     
    fkselec = root.fkselec
    jselec = root.jselec
    pselec = root.pselec
    # 根据不同的join类型, rows的计算方法不同 
    if join_type == JoinType.INNER:
        nrows = left_node.est_rows * right_node.est_rows * fkselec * jselec
    elif join_type == JoinType.LEFT:
        nrows = left_node.est_rows * right_node.est_rows * fkselec * jselec
        if nrows < left_node.est_rows:
            nrows = left_node.est_rows
        nrows = nrows * pselec
    elif join_type == JoinType.FULL:
        nrows = left_node.est_rows * right_node.est_rows * fkselec * jselec
        if nrows < left_node.est_rows:
            nrows = left_node.est_rows
        if nrows < right_node.est_rows:
            nrows = right_node.est_rows
        nrows = nrows * pselec
    elif join_type == JoinType.SEMI: #JoinType.semi的选择性定义为: LHS能够匹配上的比例
        nrows = left_node.est_rows * fkselec * jselec
    elif join_type == JoinType.ANTI:
        nrows = left_node.est_rows * (1.0 - fkselec * jselec)
        nrows = nrows * pselec
    else:
        elog("ERROR", "unrecognized join type: %s", join_type)
        nrows = 0
    
    if nrows <= 1.0:
        nrows = 1.0
    return round(nrows)


# TODO: 根据root的restrictlist得到root的jselec和pselec
def get_join_and_push_selectivity(root: PlanNodeInterface, left_node: PlanNodeInterface, d):
    pass

# TODO: 判断是否是"有效"的fkselec, 
def is_foreign_key_join_clause(rinfo: PredicateNode):
    # 其中一侧的表
    # if (bms_is_member(fkinfo->con_relid, outer_relids) &&
    #     bms_is_member(fkinfo->ref_relid, inner_relids))
    #     ref_is_outer = false;
    # else if (bms_is_member(fkinfo->ref_relid, outer_relids) &&
    #             bms_is_member(fkinfo->con_relid, inner_relids))
    #     ref_is_outer = true;
    # else
    #     continue;

    # if ((jointype == JOIN_SEMI || jointype == JOIN_ANTI) &&
    # (ref_is_outer || bms_membership(inner_relids) != BMS_SINGLETON))
    # continue;
    return False

def get_join_selectivity(root: JoinNodeInterface, left_node: PlanNodeInterface, right_node: PlanNodeInterface):
    # fkselec和jselec是连接条件,前者记录涉及外键的连接,后者是普通连接
    # pselec是选择条件
    fkselec, jselec, pselec = 1.0, 1.0, 1.0 
    # 选择条件计算
    for rinfo in root.join_filter:
        # TODO: 这里没有处理合取和析取
        # 判断是外键连接还是普通连接
        if is_foreign_key_join_clause(rinfo) == True:
            if root.join_type == JoinType.SEMI or root.join_type == JoinType.ANTI:
                # 源码实现为fkselec *= ref_rel->rows / ref_tuples, 不是很理解
                # 这里直接取1
                fkselec *= 1
            else:
                ref_rel_tuples = get_tuples_num_of_rel(rinfo.left_rel_array[0])
                fkselec *= 1.0 / math.max(ref_rel_tuples, 1.0)
        else:
            jselec *= clause_selectivity(root, rinfo, root.join_type)

    # 连接条件计算
    for rinfo in root.filter:
        pselec *= clause_selectivity(root, rinfo)
    
    return fkselec, jselec, pselec


# TODO: 返回mergejoin的startsel和endsel
def mergejoinscansel(root):
    leftstart, rightstart = 0.0, 0.0
    leftend, rightend = 1.0, 1.0
    pass

# 计算给定表格符合给定约束条件的大概元组数量
# 实现逻辑: 乘上所有的约束条件,得到selec,然后直接与笛卡尔积结果相乘,得到大致估计结果
def approx_tuple_count(root: PlanNodeInterface, clauses: List[PredicateNode]):
    selec = 1.0
    for qual in clauses:
        # pg的实现,还需要传入连接类型和special join info信息
        selec *= clause_selectivity(root, qual, root.join_type if hasattr(root, 'join_type') else None)
    return clamp_row_est(selec * root.children[0].est_rows * root.children[1].est_rows)

# 似乎是仅通过判断clause涉及的表格来判断的
# !! 这里不考虑子查询和参数化, 只判断var右边是否是表格
def treat_as_join_clause(rinfo: PredicateNode):
    return rinfo.is_join

# TODO:
def join_selectivity(root: PlanNodeInterface, rinfo: PredicateNode, join_type: JoinType):
    if rinfo.operator == Operator.EQ:
        return eqjoinsel(root, rinfo.operator, rinfo, join_type)
    elif rinfo.operator == Operator.NEQ:
        return neqjoinsel(root, rinfo.operator, rinfo, join_type)
    elif rinfo.operator == Operator.GEQ:
        return scalargejoinsel(root, rinfo)
    elif rinfo.operator == Operator.LEQ:
        return scalarlejoinsel(root, rinfo)
    elif rinfo.operator == Operator.LIKE:
        pass
    elif rinfo.operator == Operator.NOT_LIKE:
        pass
    else:
        return 0.5

# TODO:
def restriction_selectivity(root, rinfo: PredicateNode):
    if rinfo.operator == Operator.EQ:
        return eqsel(root, rinfo.operator, rinfo, False)
    elif rinfo.operator == Operator.NEQ:
        return neqsel(root, rinfo.operator, rinfo, False)
    elif rinfo.operator == Operator.GEQ:
        return scalargesel(root, rinfo)
    elif rinfo.operator == Operator.LEQ:
        return scalarlesel(root, rinfo)
    elif rinfo.operator == Operator.LIKE:
        return likesel(root, rinfo)
    elif rinfo.operator == Operator.NOT_LIKE:
        pass
    else:
        return 0.5


# 计算给定约束条件下和给定jointype的选择率
def clause_selectivity(root: PlanNodeInterface, rinfo: PredicateNode, join_type = None, simple_rel_array = None):
    s1 = 0.5

    if rinfo == None:
        return s1

    # 这里忽略了pseudoconstant的判断

    if rinfo.type == Restrict.Var:
        # !!源码判断了Var是否是当前算子的变量,按理来说不会出现
        # Var不是当前算子变量的情况,因此这里不做判断
        s1 = boolvarsel(root, rinfo, simple_rel_array)
    elif rinfo.type == Restrict.Const:
        s1 = 1.0 if (rinfo.val == None or rinfo.val == 0) else 0.0
    elif rinfo.type == Restrict.Param: # !! skip
        # !!源码判断了Param是否是可以估计的
        # 有点复杂, 这里暂时跳过
        pass
    elif rinfo.type == Restrict.NotClause:
        s1 = 1.0 - clause_selectivity(root, rinfo.children[0], join_type, simple_rel_array)
    elif rinfo.type == Restrict.AndClause:
        s1 = clauselist_selectivity(root, rinfo.children, join_type, simple_rel_array)
    elif rinfo.type == Restrict.OrClause:
        s1 = 0.0
        for ri in rinfo.children:
            s2 = clause_selectivity(root, ri, join_type, simple_rel_array)
            s1 = s1 + s2 - s1 * s2
    elif rinfo.type == Restrict.OpExpr or rinfo.type == Restrict.DistinctExpr:
        if treat_as_join_clause(rinfo):
            s1 = join_selectivity(root, rinfo, join_type)
        else:
            s1 = restriction_selectivity(root, rinfo)
        # DistinctExpr实现是=, 所以这里要取反
        if rinfo.type == Restrict.DistinctExpr:
            s1 = 1.0 - s1
    elif rinfo.type == Restrict.ScalarArrayOpExpr:
        s1 = scalararraysel(root, rinfo, treat_as_join_clause(rinfo, simple_rel_array), simple_rel_array, join_type)
    elif rinfo.type == Restrict.RowCompareExpr:    # !! skip
        pass
    elif rinfo.type == Restrict.NullTest:
        s1 = nulltestsel(root, rinfo, simple_rel_array, join_type)
    elif rinfo.type == Restrict.BooleanTest:       
        s1 = booltestsel(root, rinfo, simple_rel_array, join_type)
    elif rinfo.type == Restrict.CurrentOfExpr: 
        # 相当于主键匹配
        s1 = 1.0 / get_tuples_num_of_rel(rinfo.simple_rel)
    elif rinfo.type == Restrict.RelabelType:        # !! skip
        # s1 = clause_selectivity(root, rinfo.arg, simple_rel_array, join_type)
        pass
    elif rinfo.type == Restrict.CoerceToDomain:    # !! skip
        # s1 = clause_selectivity(root, rinfo.arg, simple_rel_array, join_type)
        pass
    else:
        s1 = boolvarsel(root, rinfo, simple_rel_array)
    
    return s1

# TODO:
def add_range_clause(rqlist, rinfo, varonleft):
    pass

def clauselist_selectivity(root: PlanNodeInterface, rinfos: List[PredicateNode], simple_rel_array = None, join_type = None):
    s1 = 1.0

    if len(rinfos) == 1:
        return clause_selectivity(root, rinfos[0], simple_rel_array, join_type)
    
    # TODO: 函数依赖处理
    # 如果可以从rinfos中发现只引用了一个表,则使用函数依赖进行处理
    rel = find_single_rel_for_clauses(root, rinfos)
    esitmatedclauses = [] # 排好序的,函数依赖已经处理过的rinfo下标
    if rel != None and rel.statlist != None:
        s1 *= dependencies_clauselist_selectivity(root, rinfos, simple_rel_array, 
                                                  join_type, rel, esitmatedclauses)
    # TODO: 其他普通的约束条件处理
    # 需要注意过滤掉上一步已经处理过的语句
    # 并将范围查询存到一个数组里,在下面统一处理
    rqlist = [] # 存储范围查询的list
    for idx, ri in enumerate(rinfos):
        if idx == esitmatedclauses[0]:
            esitmatedclauses.pop(0)
            continue
        s2 = clause_selectivity(root, ri, join_type, simple_rel_array)
        # 判断是不是范围查询rinfo
        calnow = True
        if ri.type == "OpExpr" and len(ri.args) == 2:
            ok = False
            varonleft = True
            # var op const
            # !! 这里原本还判断了左边不应该是一个易失型函数, 这里暂时跳过
            if len(ri.simple_rel_array) == 1 and len(ri.right_rel_array) == 0:
                ok = True
                varonleft = True
            elif len(ri.simple_rel_array) == 1 and len(ri.left_rel_array) == 0:
                ok = True
                varonleft = False
            
            if ok:
                if ri.opno == "<" or ri.opno == "<=":
                    add_range_clause(rqlist, ri, varonleft)
                    calnow = False
                elif ri.opno == ">" or ri.opno == ">=":
                    add_range_clause(rqlist, ri, varonleft)
                    calnow = False

        if calnow == True:
            s1 *= s2                

    # TODO: 范围查询合并处理
    for rqitem in rqlist:
        if rqitem.lobound != None and rqitem.hibound != None:
            if rqitem.lobound == DefaultSel.DEFAULT_INEQ_SEL or \
                rqitem.hibound == DefaultSel.DEFAULT_INEQ_SEL:
                s2 = DefaultSel.DEFAULT_INEQ_SEL
            else:
                s2 = rqitem.hibound + rqitem.lobound - 1.0
                # !! 这里直接将rqitem传入
                s2 += nulltestsel(root, rqitem, simple_rel_array, join_type)
                if s2 <= 0.0:
                    if s2 < -0.01:
                        s2 = DefaultSel.DEFAULT_INEQ_SEL
                    else:
                        s2 = 1.0e-10
            s1 *= s2
        else:
            if rqitem.lobound != None:
                s1 *= rqitem.lobound
            else:
                s1 *= rqitem.hibound
    
    return s1