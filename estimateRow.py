from util import *
from utilsel import *


def calc_joinrel_size_estimate(root, left_node, right_node):
    jointype = root.jointype
     
    fkselec = root.fkselec
    jselec = root.jselec
    pselec = root.pselec
    # 根据不同的join类型, rows的计算方法不同 
    if jointype == "JOIN_INNER":
        nrows = left_node.rows * right_node.rows * fkselec * jselec
    elif jointype == "JOIN_LEFT":
        nrows = left_node.rows * right_node.rows * fkselec * jselec
        if nrows < left_node.rows:
            nrows = left_node.rows
        nrows = nrows * pselec
    elif jointype == "JOIN_FULL":
        nrows = left_node.rows * right_node.rows * fkselec * jselec
        if nrows < left_node.rows:
            nrows = left_node.rows
        if nrows < right_node.rows:
            nrows = right_node.rows
        nrows = nrows * pselec
    elif jointype == "JOIN_SEMI": # join_semi的选择性定义为: LHS能够匹配上的比例
        nrows = left_node.rows * fkselec * jselec
    elif jointype == "JOIN_ANTI":
        nrows = left_node.rows * (1.0 - fkselec * jselec)
        nrows = nrows * pselec
    else:
        elog("ERROR", "unrecognized join type: %s", jointype)
        nrows = 0
    
    if nrows <= 1.0:
        nrows = 1.0
    return round(nrows)


# TODO: 根据root的restrictlist得到root的jselec和pselec
def get_join_and_push_selectivity(root, left_node, d):
    pass

# TODO: 判断是否是"有效"的fkselec
def is_foreign_key_join_clause(rinfo: DerivedRestrictInfo):
    # if rinfo.left_rel_array[0]
    pass



def get_join_selectivity(root: DerivedJoinNode, left_node: DerivedPlanNode, right_node: DerivedPlanNode):
    # fkselec和jselec是连接条件,前者记录涉及外键的连接,后者是普通连接
    # pselec是选择条件
    fkselec, jselec, pselec = 1.0, 1.0, 1.0 
    # 选择条件计算
    for rinfo in root.join_filter:
        # TODO: 这里没有处理合取和析取
        # 判断是外键连接还是普通连接
        if is_foreign_key_join_clause(rinfo) == True:
            if root.join_type == "SEMI" or root.join_type == "ANTI":
                # 源码实现为fkselec *= ref_rel->rows / ref_tuples, 不是很理解
                # 这里直接取1
                fkselec *= 1
            else:
                ref_rel_tuples = get_tuples_num(rinfo.left_rel_array[0])
                fkselec *= 1.0 / math.max(ref_rel_tuples, 1.0)
        else:
            jselec *= clause_selectivity(root, rinfo)

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
def approx_tuple_count(root, clauses):
    selec = 1.0
    for qual in clauses:
        # pg的实现,还需要传入连接类型和special join info信息
        selec *= clause_selectivity(root, qual)
    return clamp_row_est(selec * root.children[0].rows * root.children[1].rows)

# TODO: 似乎是仅通过判断clause涉及的表格来判断的
def treat_as_join_clause(rinfo, simple_rel_array):
    if len(simple_rel_array) > 1:
        return True
    return False

# TODO:
def join_selectivity(root, rinfo, jointype):
    pass

# TODO:
def restriction_selectivity(root, rinfo):
    # 根据不同的
    pass


# TODO: 计算给定约束条件下的选择率
def clause_selectivity(root, rinfo: DerivedRestrictInfo, simple_rel_array, jointype):
    s1 = 0.5

    if rinfo == None:
        return s1

    # 这里忽略了pseudoconstant的判断

    if rinfo.type == "Var":
        # !!源码判断了Var是否是当前算子的变量,按理来说不会出现
        # Var不是当前算子变量的情况,因此这里不做判断
        s1 = boolvarsel(root, rinfo, simple_rel_array)
    elif rinfo.type == "Const":
        s1 = 1.0 if (rinfo.val == None or rinfo.val == 0) else 0.0
    elif rinfo.type == "Param":
        # !!源码判断了Param是否是可以估计的
        # 有点复杂, 这里不进行判断, 直接跳过
        pass
    elif rinfo.type == "NotClause":
        # rinfo.type = rinfo.
        s1 = 1.0 - clause_selectivity(root, rinfo.arg, simple_rel_array, jointype)
    elif rinfo.type == "AndClause":
        s1 = clauselist_selectivity(root, rinfo.args, simple_rel_array, jointype)
    elif rinfo.type == "OrClause":
        s1 = 0.0
        for ri in rinfo.args:
            s2 = clause_selectivity(root, ri, simple_rel_array, jointype)
            s1 = s1 + s2 - s1 * s2
    elif rinfo.type == "OpExpr" or rinfo.type == "DistinctExpr":
        if treat_as_join_clause(rinfo, simple_rel_array):
            s1 = join_selectivity(root, rinfo, jointype)
        else:
            s1 = restriction_selectivity(root, rinfo)
        # DistinctExpr实现是=, 所以这里要取反
        if rinfo.type == "DistinctExpr":
            s1 = 1.0 - s1
    elif rinfo.type == "ScalarArrayOpExpr":
        s1 = scalararraysel(root, rinfo, treat_as_join_clause(rinfo, simple_rel_array), simple_rel_array, jointype)
    elif rinfo.type == "RowCompareExpr":    # !! 不常用,暂时不实现
        pass
    elif rinfo.type == "NullTest":
        s1 = nulltestsel(root, rinfo, simple_rel_array, jointype)
    elif rinfo.type == "BooleanTest":       
        s1 = booltestsel(root, rinfo, simple_rel_array, jointype)
    elif rinfo.type == "CurrentOfExpr": 
        # 相当于主键匹配
        s1 = 1.0 / get_tuples_num(rinfo.simple_rel)
    elif rinfo.type == "RelabelType":
        s1 = clause_selectivity(root, rinfo.arg, simple_rel_array, jointype)
    elif rinfo.type == "CoerceToDomain":    # !! 似乎用不到, 这里没有深究,随便参照源码实现了一下
        s1 = clause_selectivity(root, rinfo.arg, simple_rel_array, jointype)
    else:
        s1 = boolvarsel(root, rinfo, simple_rel_array)

# TODO:
def addRangeClause(rqlist, rinfo, varonleft):
    pass

def clauselist_selectivity(root, rinfos: List[DerivedRestrictInfo], simple_rel_array, jointype):
    s1 = 1.0

    if len(rinfos) == 1:
        return clause_selectivity(root, rinfos[0], simple_rel_array, jointype)
    
    # TODO: 函数依赖处理
    # 如果可以从rinfos中发现只引用了一个表,则使用函数依赖进行处理
    rel = find_single_rel_for_clauses(root, rinfos)
    esitmatedclauses = [] # 排好序的,函数依赖已经处理过的rinfo下标
    if rel != None and rel.statlist != None:
        s1 *= dependencies_clauselist_selectivity(root, rinfos, simple_rel_array, 
                                                  jointype, rel, esitmatedclauses)
    # TODO: 其他普通的约束条件处理
    # 需要注意过滤掉上一步已经处理过的语句
    # 并将范围查询存到一个数组里,在下面统一处理
    rqlist = [] # 存储范围查询的list
    for idx, ri in enumerate(rinfos):
        if idx == esitmatedclauses[0]:
            esitmatedclauses.pop(0)
            continue
        s2 = clause_selectivity(root, ri, simple_rel_array, jointype)
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
                    addRangeClause(rqlist, ri, varonleft)
                    calnow = False
                elif ri.opno == ">" or ri.opno == ">=":
                    addRangeClause(rqlist, ri, varonleft)
                    calnow = False

        if calnow == True:
            s1 *= s2                

    # TODO: 范围查询合并处理
    for rqitem in rqlist:
        if rqitem.lobound != None and rqitem.hibound != None:
            if rqitem.lobound == DefaultSel.DEFAULT_INEQ_SEL.value or \
                rqitem.hibound == DefaultSel.DEFAULT_INEQ_SEL.value:
                s2 = DefaultSel.DEFAULT_INEQ_SEL.value
            else:
                s2 = rqitem.hibound + rqitem.lobound - 1.0
                # !! 这里直接将rqitem传入
                s2 += nulltestsel(root, rqitem, simple_rel_array, jointype)
                if s2 <= 0.0:
                    if s2 < -0.01:
                        s2 = DefaultSel.DEFAULT_INEQ_SEL.value
                    else:
                        s2 = 1.0e-10
            s1 *= s2
        else:
            if rqitem.lobound != None:
                s1 *= rqitem.lobound
            else:
                s1 *= rqitem.hibound
    
    return s1