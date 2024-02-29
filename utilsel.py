# Operator
from enum import Enum
from Recost_tmp.PlanNode.planNodeAPI import *
from Recost_tmp.PlanNode.filterNode import PredicateNode, Operator, Restrict
from Recost_tmp.util import * 
import math

# !! 未实现的操作符选择性(测试集中未包含操作符)
# 几何操作符
# 数组操作符(数组之间的比较)
# 网络操作符
# 范围操作符
# 

# TODO: 对于不同的操作符族,暂时没有考虑不同数据类型对运算符计算原理和选择性计算的影响

StatisticInfoOfRel = {}


class DefaultSel:
    DEFAULT_EQ_SEL = 0.005                  # A = b
    DEFAULT_INEQ_SEL = 0.3333333333333333   # A < b
    DEFAULT_RANGE_INEQ_SEL = 0.005          # A > b and A < c
    DEFAULT_MATCH_SEL = 0.005               # A like b



# TODO: 从统计信息获取rel的tuple数量
def get_tuples_num(rel):
    pass
# TODO:
def find_single_rel_for_clauses(root, rinfos):
    pass
# TODO:
def dependencies_clauselist_selectivity(root, rinfo, simple_rel_array, join_type, rel, esitmatedclauses):
    pass


def get_statistic_of_rel(relation: PredicateNode):
    # 检查是不是
    if len(relation.children) == 0 and relation.is_table_exist == True:
        # TODO: 约定text第一个维度为表格全称, 第二个维度为属性全称
        return StatisticInfoOfRel.get(relation.text[0], StatisticInfo())
    # 返回一个空的统计信息
    return StatisticInfo()

def get_statistic_of_col(rel_sta: StatisticInfo, column: PredicateNode):
    if len(column.children) == 0 and column.is_table_exist == True:
        # TODO: 约定text第一个维度为表格全称, 第二个维度为属性全称
        return rel_sta.column_sta.get(column.text[1], None)
    # 返回None
    return None

def get_statistic_kind_of(col_sta: 'ColumnStatisticInfo', kind):
    staidx = -1
    for idx, val in enumerate(col_sta.stakind):
        if val == kind:
            staidx = idx
            break
    if staidx != -1:
        return staidx, col_sta.staop[staidx], col_sta.stanumbers[staidx], col_sta.stavalues[staidx]
    return staidx, None, None, None

# 根据列的统计信息返回预估的不同tuple的数量
# !! 参照源码的简单实现
# 返回值为ndistinct, isdefault
def get_variable_num_distinct(rel_sta: 'StatisticInfo',col_sta: 'ColumnStatisticInfo'):
    stadistinct = -1.0
    if rel_sta == None or col_sta == None:
        # !! 这里按照自己的理解设计的,没有列统计信息的话直接返回默认值
        return math.min(DefaultVal.DEFAULT_NUM_DISTINCT, clamp_row_est(rel_sta.tuples)), True

    stadistinct = col_sta.distinct
    stanullfrac = col_sta.nullfrac
    
    # 正值为数量
    if stadistinct > 0.0:
        return clamp_row_est(stadistinct), False

    # 负值为比例
    return clamp_row_est(rel_sta.tuples * -stadistinct), False

def judge_unique(col_sta: 'ColumnStatisticInfo'):
    dist = col_sta.distinct - col_sta.nullfrac - ColumnStatisticInfo.UNIQUE_DISTINCT
    # 误差范围内则认为是unique
    if dist < 0.01 and dist > -0.01:
        return True
    return False

# 根据给定的统计信息判断等于给定值的选择率
# !! const_val 要统一设置为字符串类型吗
# !! 需要判断onleft的情况吗?
def var_eq_const(rel_sta: StatisticInfo, column_name ,constval, constisnull, negate):
    # 等于操作符是严格的,不可以接受NULL参数,如果为NULL,直接返回0.0
    if constisnull == True:
        return 0.0

    # 得到column statistic
    col_sta = rel_sta.column_sta.get(column_name, None)
    if col_sta == None:
        nullfrac = 0.0
        selec = DefaultSel.DEFAULT_EQ_SEL
    else:
        nullfrac = col_sta.nullfrac

        # 判断是不是unique
        if judge_unique(col_sta) and rel_sta.tuples >= 1.0:
            selec = 1.0 / rel_sta.tuples
        else:
        
            # 判断是不是mcv里的
            flag = False    # 判断是不是mcv里的
            
            staidx, _, stanumbers, stavalues = get_statistic_kind_of(col_sta, StatisticKind.STATISTIC_KIND_MCV)
            
            mcvnum = 0
            if staidx != -1:
                mcvnum = len(stavalues)
                
                sumcommon = 0.0
                for idx, val in enumerate(stavalues):
                    if val == constval: # 当前mcv是不是constval
                        # 如果是,直接返回对应的比例
                        selec = stanumbers[idx]
                        flag = True
                        break
                    # 如果不是,则记录到sumcommon中, 用于后续使用
                    sumcommon += stanumbers[idx]
            
            # mcv里找不到, 视为普通的元素, 和其他普通元素共享剩余的选择性
            if flag == False:
                selec = 1.0 - sumcommon - nullfrac
                selec = clamp_probability(selec)
                otherdistinct, _ = get_variable_num_distinct(rel_sta, col_sta) - mcvnum
                if otherdistinct > 1:
                    selec /= otherdistinct
                
                # 如果存在mcv的话,还有进行一下检测
                if staidx != -1:
                    if mcvnum > 0 and selec > stanumbers[mcvnum - 1]:
                        selec = stanumbers[mcvnum - 1]
    
    if negate == True:
        selec = 1.0 - selec - nullfrac
    selec = clamp_probability(selec)
    return selec

# var = something-other-than-const case: 通常来说other都是var,不过我们不会用到other的统计信息
# 而只是通过左侧表格的统计信息简单的进行估计
def var_eq_non_const(rel_sta: StatisticInfo, column_name, negate):
    col_sta = rel_sta.column_sta.get(column_name, None)
    if col_sta == None:
        nullfrac = 0.0
        selec = DefaultSel.DEFAULT_EQ_SEL
    else:
        nullfrac = col_sta.nullfrac
    
        # 判断是不是unique
        if judge_unique(col_sta) and rel_sta.tuples >= 1.0:
            selec = 1.0 / rel_sta.tuples
        else:
            # 直接做均摊
            selec = 1.0 - nullfrac
            ndistinct, _ = get_variable_num_distinct(rel_sta, col_sta)
            if ndistinct > 1:
                selec /= ndistinct
            
            # 进行检查, selec不应该大于mcv第一个值
            staidx, _, stanumbers, stavalues = get_statistic_kind_of(col_sta, StatisticKind.STATISTIC_KIND_MCV)

            if staidx != -1:
                mcvnum = len(stavalues)
                if mcvnum > 0 and selec > stanumbers[0]:
                    selec = stanumbers[0]
    
    if negate == True:
        selec = 1.0 - selec - nullfrac
    selec = clamp_probability(selec)
    return selec
    

# 判断bool变量的选择性
def boolvarsel(root, rinfo: PredicateNode, simple_rel_array):
    rel_sta = get_statistic_of_rel(rinfo)
    if rel_sta != None:
        # !! 如果const_val统一为str类型,那么应该输入"True”
        selec = var_eq_const(rel_sta, rinfo.column_name, True, False, False)
    elif rinfo.type == "FuncExpr":
        selec = 0.3333333
    else:
        selec = 0.5
    return selec


# TODO: ScalarArrayOpExpr   ANY ALL
def scalararraysel(root, rinfo, is_join_clause, simple_rel_array, join_type):
    pass

# TODO: RowCompareExpr
def rowcomparesel(root, rinfo, simple_rel_array, join_type):
    pass

# TODO: nulltestsel
def nulltestsel(root, rinfo, simple_rel_array, join_type):
    pass

# TODO: booltestsel
def booltestsel(root, rinfo, simple_rel_array, join_type):
    pass


# op: = of restrict
# eq的选择性: 包含var = const 和 var = other两种情况
def eqsel(root, operator, rinfo: PredicateNode, negate): # negate表示取的结果是!operator的
    if negate == True: # 通过neqsel调用的
        # 转为operator
        # !!实际上用不到operator,但是目前来说调用的只有 == 和 !=, 所以还没问题
        operator = get_negator(operator)
        if operator == Operator.IllegalOperator:
            return 1.0 - DefaultSel.DEFAULT_EQ_SEL

    if rinfo.children[0].type != Restrict.Const and rinfo.children[1].type != Restrict.Const: 
        # 两边没有一个是const
        if rinfo.children[0].type == Restrict.Var:
            rel_sta = get_statistic_of_rel(rinfo.children[0])
            col_name = rinfo.args[0].col_name
        else:
            rel_sta = get_statistic_of_rel(rinfo.children[1])
            col_name = rinfo.args[1].col_name
        if rel_sta == None:
            return (1.0 - DefaultSel.DEFAULT_EQ_SEL.val) if negate == True else DefaultSel.DEFAULT_EQ_SEL.val
        return var_eq_non_const(rel_sta, col_name, negate)
    

    if rinfo.children[0].type == Restrict.Var:
        rel_sta = get_statistic_of_rel(rinfo.children[0])
        col_name = rinfo.args[0].col_name
        const_val = rinfo.args[1].val
        const_is_null = rinfo.args[1].is_null
    else:
        rel_sta = get_statistic_of_rel(rinfo.children[1])
        col_name = rinfo.args[1].col_name
        const_val = rinfo.args[0].val
        const_is_null = rinfo.args[0].is_null
    # 如果没有统计信息,返回默认值
    if rel_sta == None:
        return (1.0 - DefaultSel.DEFAULT_EQ_SEL.val) if negate == True else DefaultSel.DEFAULT_EQ_SEL.val
    return var_eq_const(rel_sta, col_name, const_val, const_is_null, negate)
    
# op: = of join
def eqjoinsel(root: PlanNodeInterface, operator: Operator, rinfo: PredicateNode, join_type: JoinType):
    if join_type == JoinType.INNER or join_type == JoinType.LEFT or \
        join_type == JoinType.FULL:
        selec = eqjoinsel_inner(operator, rinfo)
    elif join_type == JoinType.SEMI or join_type == JoinType.ANTI:
        # 源码这里会判断是否进行了reversed
        selec = eqjoinsel_semi(operator, rinfo)
    else:
        elog("ERROR", "unrecognized join type: %s", join_type)
        selec = 0
    selec = clamp_probability(selec)
    return selec

# 没有使用operator变量,这里默认都是=, 不确定会不会出问题
def eqjoinsel_inner(operator: Operator, rinfo: PredicateNode):
    # 先处理mcv,再按照普通匹配处理
    flag = True # 判断能否按照先处理mcv,再处理其他数据的进程进行
    rel_sta0 = get_statistic_of_rel(rinfo.children[0])
    rel_sta1 = get_statistic_of_rel(rinfo.children[1])
    col_sta0 = get_statistic_of_col(rel_sta0, rinfo.children[0])
    col_sta1 = get_statistic_of_col(rel_sta1, rinfo.children[1])

    # 是否存在列统计信息
    if col_sta0 == None or col_sta1 == None:
        flag = False
    # 列统计信息是否有MCV
    if flag == True:
        staidx0, _, stanumbers0, stavalues0 = get_statistic_kind_of(col_sta0, StatisticKind.STATISTIC_KIND_MCV)
        staidx1, _, stanumbers1, stavalues1 = get_statistic_kind_of(col_sta1, StatisticKind.STATISTIC_KIND_MCV)
        if staidx0 == -1 or staidx1 == -1:
            flag = False

    # 
    if flag == True:
        hasmatch0 = [False] * len(stavalues0)
        hasmatch1 = [False] * len(stavalues1)
        nullfrac0 = col_sta0.nullfrac
        nullfrac1 = col_sta1.nullfrac
        nd0, _ = get_variable_num_distinct(rel_sta0, col_sta0)
        nd1, _ = get_variable_num_distinct(rel_sta1, col_sta1)

        matchprodfreq = 0.0
        nmatches = 0
        for idx0, val0 in enumerate(stavalues0):
            for idx1, val1 in enumerate(stavalues1):
                if val0 == val1:
                    hasmatch0[idx0] = hasmatch1[idx1] = True
                    matchprodfreq += stanumbers0[idx0] * stanumbers1[idx1]
                    nmatches += 1
                    break
        
        matchprodfreq = clamp_probability(matchprodfreq)
        matchfreq0, unmatchfreq0 = 0.0, 0.0
        for idx0, val0 in enumerate(stavalues0):
            if hasmatch0[idx0] == True:
                matchfreq0 += stanumbers0[idx0]
            else:
                unmatchfreq0 += stanumbers0[idx0]
        matchfreq0 = clamp_probability(matchfreq0)
        unmatchfreq0 = clamp_probability(unmatchfreq0)

        matchfreq1, unmatchfreq1 = 0.0, 0.0
        for idx1, val1 in enumerate(stavalues1):
            if hasmatch1[idx1] == True:
                matchfreq1  += stanumbers1[idx1]
            else:
                unmatchfreq1 += stanumbers1[idx1]
        matchfreq1 = clamp_probability(matchfreq1)
        unmatchfreq1 = clamp_probability(unmatchfreq1)

        otherfreq0 = 1.0 - nullfrac0 - matchfreq0 - unmatchfreq0
        otherfreq1 = 1.0 - nullfrac1 - matchfreq1 - unmatchfreq1
        otherfreq0 = clamp_probability(otherfreq0)
        otherfreq1 = clamp_probability(otherfreq1)

        totalsel0 = matchprodfreq
        if nd1 > len(stavalues1): # 表0没有匹配的mcv和表1的其他值匹配
            totalsel0 += unmatchfreq0 * otherfreq1 / (nd1 - len(stavalues1))
        if nd1 > nmatches:  # 表0其他值和表1的未匹配mcv和其他值匹配
            totalsel0 += otherfreq0 * (otherfreq1 + unmatchfreq1) / (nd1 - nmatches)
        
        totalsel1 = matchprodfreq
        if nd0 > len(stavalues0): # 表1没有匹配的mcv和表0的其他值匹配
            totalsel1 += unmatchfreq1 * otherfreq0 / (nd0 - len(stavalues0))
        if nd0 > nmatches:  # 表1其他值和表0的未匹配mcv和其他值匹配
            totalsel1 += otherfreq1 * (otherfreq0 + unmatchfreq0) / (nd0 - nmatches)

        selec = (totalsel0 if totalsel0 < totalsel1 else totalsel1)

    else:
        if col_sta0 != None:
            nullfrac0 = col_sta0.nullfrac
            nd0, _ = get_variable_num_distinct(rel_sta0, col_sta0)
        else:
            nullfrac0 = 0.0
            nd0 = DefaultVal.DEFAULT_NUM_DISTINCT
        
        if col_sta1 != None:
            nullfrac1 = col_sta1.nullfrac
            nd1, _ = get_variable_num_distinct(rel_sta1, col_sta1)
        else:
            nullfrac1 = 0.0
            nd1 = DefaultVal.DEFAULT_NUM_DISTINCT
        
        selec = (1.0 - nullfrac0) * (1.0 - nullfrac1)
        if nd0 > nd1:
            selec /= nd0
        else:
            selec /= nd1
    
    return selec

# 没有使用operator变量,这里默认都是=, 不确定会不会出问题
def eqjoinsel_semi(operator, rinfo):
    flag = True
    rel_sta0 = get_statistic_of_rel(rinfo.children[0])
    rel_sta1 = get_statistic_of_rel(rinfo.children[1])
    col_sta0 = get_statistic_of_col(rel_sta0, rinfo.children[0])
    col_sta1 = get_statistic_of_col(rel_sta1, rinfo.children[1])
    
    if col_sta0 == None or col_sta1 == None:
        flag = False
    
    if flag == True:
        staidx0, _, stanumbers0, stavalues0 = get_statistic_kind_of(col_sta0, StatisticKind.STATISTIC_KIND_MCV)
        staidx1, _, stanumbers1, stavalues1 = get_statistic_kind_of(col_sta1, StatisticKind.STATISTIC_KIND_MCV)
        if staidx0 == -1 or staidx1 == -1:
            flag = False
    
    if flag == True:
        nullfrac0 = col_sta0.nullfrac
        nullfrac1 = col_sta1.nullfrac
        nd0, isdefault0 = get_variable_num_distinct(rel_sta0, col_sta0)
        nd1, isdefault1 = get_variable_num_distinct(rel_sta1, col_sta1)
        # 源码这里进行了nd1修正, nd1 = min(nd1, rel_rows)
        # !! 不是很理解rows和tuples的区别, 这里直接跳过了

        # 源码由于修正了nd1, 所以这里对stavalues进行了修正
        # 由于我们前面没有修正nd1, 所以这里我们也不做修改
        hasmatch0 = [False] * len(stavalues0)
        hasmatch1 = [False] * len(stavalues1)

        nmatches = 0
        for idx0, val0 in enumerate(stavalues0):
            for idx1, val1 in enumerate(stavalues1):
                if hasmatch1[idx1] == True: # Join SEMI ANTI的特性
                    continue
                if val0 == val1:
                    hasmatch0[idx0] = hasmatch1[idx1] = True
                    nmatches += 1
                    break

        # 记录左表成功匹配的比例
        matchfreq0 = 0.0
        for idx0, val0 in enumerate(stavalues0):
            if hasmatch0[idx0] == True:
                matchfreq0 += stanumbers0[idx0]
        matchfreq0 = clamp_probability(matchfreq0)

        # 剩余tuples的匹配, 认为左表的每一列都能匹配到
        if isdefault0 == False and isdefault1 == False:
            nd0 -= nmatches
            nd1 -= nmatches
            if nd0 <= nd1 or nd1 < 0:
                uncertainfrac = 1.0
            else:
                uncertainfrac = nd1 / nd0
        else:
            uncertainfrac = 0.5
        uncertain = 1.0 - matchfreq0 - nullfrac0
        uncertain = clamp_probability(uncertain)
        selec = matchfreq0 + uncertainfrac * uncertain
    else:
        # 
        if col_sta0 != None:
            nullfrac0 = col_sta0.nullfrac
            nd0, isdefault0 = get_variable_num_distinct(rel_sta0, col_sta0)
        else:
            nullfrac0 = 0.0
            nd0 = DefaultVal.DEFAULT_NUM_DISTINCT
            isdefault0 = True
        if col_sta1 != None:
            nullfrac1 = col_sta1.nullfrac
            nd1, isdefault1 = get_variable_num_distinct(rel_sta1, col_sta1)
        else:
            nullfrac1 = 0.0
            nd1 = DefaultVal.DEFAULT_NUM_DISTINCT
            isdefault1 = True
        
        if isdefault0 == False and isdefault1 == False:
            if nd0 <= nd1 or nd1 < 0:
                selec = 1.0 - nullfrac0
            else:
                selec = (nd1 / nd0) * (1.0 - nullfrac1)
        else:
            selec = 0.5 * (1 - nullfrac0)
    
    return selec



# op: <> of restrict
def neqsel(root, operator, rinfo: PredicateNode):
    return eqsel(root, operator, rinfo, True)

# op: <> of join
def neqjoinsel(root, operator, rinfo: PredicateNode, join_type):
    if join_type == JoinType.SEMI or join_type == JoinType.ANTI:
        # 这两种JOIN, 只要右表不是全部一个值,就是相同的, 通常来说这是成立的
        # 所以只需要将外表的null去掉即可
        rel_sta = get_statistic_kind_of(rinfo.children[0])
        col_sta = get_statistic_of_col(rel_sta, rinfo.children[0])
        if col_sta != None:
            nullfrac = col_sta.nullfrac
        else:
            nullfrac = 0.0
        selec = 1.0 - nullfrac
    else:
        # 从<>切换到==
        operator = get_negator(operator)
        if operator == Operator.IllegalOperator:
            selec = DefaultSel.DEFAULT_EQ_SEL
        else:
            # 调用eqjoinsel
            selec = eqjoinsel(root, operator, rinfo, join_type)
        selec = 1.0 - selec

    return selec


# 计算mcv中有多少满足 var op constval是返回True
def mcv_selectivity(rel_sta, col_sta, operator, constval, varonleft):
    mcv_selec = 0.0
    sumcommon = 0.0

    staidx, _, stanumbers, stavalues = get_statistic_kind_of(col_sta, StatisticKind.STATISTIC_KIND_MCV)
    if staidx != -1:
        for idx, val in enumerate(stavalues):
            if varonleft == True:
                if get_result_of_operator(val, constval, operator) == True:
                    mcv_selec += stanumbers[idx]
            else:
                if get_result_of_operator(constval, val, operator) == True:
                    mcv_selec += stanumbers[idx]
            sumcommon += stanumbers[idx]
    return mcv_selec, sumcommon



# 通过hist估计<=的选择率
def ineq_histogram_selectivity(root, rel_sta, col_sta: ColumnStatisticInfo, operator, isgt, iseq, constval):
    # 小于0的选择率是非法值, 调用者可以通过这个进行判断是否正确通过histogram得到了selec
    hist_selec = -1.0 

    staidx, _, stanumbers, stavalues = get_statistic_kind_of(col_sta, StatisticKind.STATISTIC_KIND_HISTOGRAM)
    nvalues = len(stavalues)
    if staidx != -1:
        if nvalues > 1:
            histfrac = 0.0
            lobound = 0                 # first(0-based)
            hibound = nvalues    # last+1(0-based)
            have_end = False
            
            # 为了避免落入最左和最右的区间,我们需要计算have_end, 即重新更新极端值
            # 当只有两个值时,索性先直接更新极端值了
            if nvalues == 2:
                have_end, tmp_min, tmp_max = get_actual_variable_range(col_sta)
                if have_end == True:
                    stavalues[0] = tmp_min
                    stavalues[1] = tmp_max
            
            # 二分找constval落在哪一个hist中
            while lobound < hibound:
                probe = (lobound + hibound) // 2
                
                if probe == 0 and nvalues > 2: # 取到极端数据了,得到一下实际的最大值和最小值,再进行判断
                    have_end, tmp_min, _ = get_actual_variable_range(col_sta)
                    if have_end == True:
                        stavalues[0] = tmp_min
                elif probe == nvalues - 1 and nvalues > 2:
                    have_end, _, tmp_max = get_actual_variable_range(col_sta)
                    if have_end == True:
                        stavalues[probe] = tmp_max
                
                # 记录运算的结果
                ltcmp = get_result_of_operator(stavalues[probe], constval, operator)
                if isgt: # 操作符是isgt的话,需要将结果取反
                    ltcmp = not ltcmp
                
                if ltcmp == True:
                    lobound = probe + 1
                else:
                    hibound = probe
            
            if lobound <= 0: # 落在第一个还前
                histfrac = 0.0
            elif lobound >= nvalues: # 落在最后一个还后
                histfrac = 1.0
            else:
                i = lobound
                eq_selec = 0
                val, high, low = 0, 0, 0
                binfrac = 0
                # 落在第一个hist, 或者为>=或者<
                if i == 1 or isgt == iseq: 
                    otherdistinct, isdefault = get_variable_num_distinct(rel_sta, col_sta)

                    staidx_t, _, _, stanumbers_t = get_statistic_kind_of(col_sta, StatisticKind.STATISTIC_KIND_MCV)
                    if staidx_t != -1:
                        otherdistinct -= len(stanumbers_t)
                    
                    # 需要额外减去的部分
                    if otherdistinct > 1:
                        eq_selec = 1.0 / otherdistinct
                
                success, val, low, high = convert_to_scalar(constval, stavalues[i - 1], stavalues[i], col_sta)
                if success == True:
                    if high <= low:
                        binfrac = 0.5
                    elif val <= low:
                        binfrac = 0.0
                    elif val >= high:
                        binfrac = 1.0
                    else:
                        binfrac = (val - low) / (high - low)
                        if math.isnan(binfrac) == True or \
                            binfrac < 0.0 or binfrac > 1.0:
                            binfrac = 0.5
                else:
                    binfrac = 0.5
                
                histfrac = (i - 1) + binfrac
                histfrac /= nvalues - 1

                # 源码中说这一部分是第一个hist会略窄,需要加上
                # ?不是很懂
                if i == 1:
                    histfrac += eq_selec * (1.0 - binfrac)
                if isgt == iseq: # >= 和 < 需要减去这部分的内容
                    histfrac -= eq_selec

            hist_selec = (1.0 - histfrac) if isgt else histfrac

            if have_end == True:
                hist_selec = clamp_probability(hist_selec)
            else:
                # 对于极端值, 而又没有实际最值的数据,不要贸然取0或1
                cutoff = 0.01 / (nvalues - 1)
                if hist_selec < cutoff:
                    hist_selec = cutoff
                elif hist_selec > 1.0 - cutoff:
                    hist_selec = 1.0 - cutoff
    
    return hist_selec

# 
def scalarineqsel(root, operator, isgt, iseq, rel_sta, col_sta, constval):
    mcv_selec, sumcommon = mcv_selectivity(rel_sta, col_sta, operator, constval, True)

    hist_selec = ineq_histogram_selectivity(root, rel_sta, col_sta, operator, isgt, iseq, constval)

    selec = 1.0 - col_sta.nullfrac - sumcommon

    if hist_selec >= 0.0:
        selec *= hist_selec
    else:
        selec *= 0.5

    selec += mcv_selec

    selec = clamp_probability(selec)
    return selec

# op: < >= > >= of restrict
def scalarineqsel_wrapper(root: PlanNodeInterface, rinfo: PredicateNode, isgt, iseq):
    operator = rinfo.opno
    # 判断一下var在左边还是右边
    if rinfo.args[0].type == "Var":
        onleft = True
        rel_sta = get_statistic_of_rel(rinfo.children[0])
        col_sta = get_statistic_of_col(rel_sta, rinfo.children[0])
        # 只能处理另一端是Const的情况
        # 只能处理有列统计信息的情况
        if rinfo.args[1].type != "Const" or col_sta == None:
            return DefaultSel.DEFAULT_INEQ_SEL
        constval = rinfo.args[1].val
    else:
        onleft = False
        rel_sta = get_statistic_of_rel(rinfo.children[1])
        col_sta = get_statistic_of_col(rel_sta, rinfo.children[1])
        if rinfo.args[0].type != "Const" or col_sta == None:
            return DefaultSel.DEFAULT_INEQ_SEL
        constval = rinfo.args[0].val
    
    # 切换到var在左边
    if onleft == False:
        operator = get_commutator(operator)
        if operator == Operator.IllegalOperator:
            return DefaultSel.DEFAULT_INEQ_SEL
        isgt = not isgt

    selec = scalarineqsel(root, operator, isgt, iseq, rel_sta, col_sta, constval)

# op: < of restrict
def scalarltsel(root, rinfo):
    scalarineqsel_wrapper(root, rinfo, False, False)
# op: <= of restrict
def scalarlesel(root, rinfo):
    scalarineqsel_wrapper(root, rinfo, False, True)
# op: > of restrict
def scalargtsel(root, rinfo):
    scalarineqsel_wrapper(root, rinfo, True, False)
# op: >= of restrict
def scalargesel(root, rinfo):
    scalarineqsel_wrapper(root, rinfo, True, True)

# 难以估计,全取默认值
# op: < of join
def scalarltjoinsel(root, rinfo):
    return DefaultSel.DEFAULT_INEQ_SEL
# op: <= of join
def scalarlejoinsel(root, rinfo):
    return DefaultSel.DEFAULT_INEQ_SEL# op: < of join
# op: > of join
def scalargtjoinsel(root, rinfo):
    return DefaultSel.DEFAULT_INEQ_SEL# op: < of join
# op: >= of join
def scalargejoinsel(root, rinfo):
    return DefaultSel.DEFAULT_INEQ_SEL

# Pattern　Match
# TODO:
def histogram_selectivity(rel_sta, col_sta, operator, constval):
    pass
# TODO:
def prefix_selectivity(rel_sta, col_sta, constval):
    pass

class PatternType(Enum):
    PATTERN_TYPE_LIKE = 0
class PatternPrefixStatus(Enum):
    PATTERN_PREFIX_NONE = 0 
    PATTERN_PREFIX_PARTIAL = 1
    PATTERN_PREFIX_EXACT = 2

# TODO: 获取固定前缀
def pattern_fixed_prefix(constval):
    pass

# 
def patternsel(root, rinfo: PredicateNode, ptype: PatternType, negate):
    # 似乎所有的信息都写进了rinfo中, 这里没有像源码一样将operator作为参数传进来
    # !! 后续仔细验证一下是否会出错
    operator = rinfo.opno
    if negate == True:
        operator = get_negator(operator)
        if operator == Operator.IllegalOperator:
            elog("ERROR", "patternsel called for operator without a negator")
        result = 1.0 - DefaultSel.DEFAULT_MATCH_SEL
    else:
        restrictParse = DefaultSel.DEFAULT_MATCH_SEL
    
    # must var op const
    if rinfo.args[0].type != "Var" or rinfo.args[1].type != "Const":
        return result
    
    # TODO: None判断,暂时没在args处理好,后面考虑实现
    if rinfo.args[1].constisnull == True:
        return 0.0
    
    
    constval = rinfo.args[1].val
    consttype = rinfo.args[1].val_type # TODO: 暂时没有考虑不同数据类型的影响

    # 源码在这里本来有consttype的判断


    # get nullfrac
    rel_sta = get_statistic_of_rel(rinfo.children[0])
    col_sta = get_statistic_of_col(rel_sta, rinfo.children[0])

    nullfrac = (col_sta.nullfrac if col_sta != None else 0.0)

    pstatus, prefix, rest_selec = pattern_fixed_prefix(constval)
    
    # 源码这里对vartype和consttype进行了判断
    # 并进行了转化(主要是text和byte两种)
    # 这里暂时跳过(考虑不实现)

    # 如果是exact, 则与=无异
    if pstatus == PatternPrefixStatus.PATTERN_PREFIX_EXACT:
        # 源码这里获取了比较的操作符族
        # 这里暂时跳过(考虑不实现)
        result = var_eq_const(rel_sta, rinfo.args[0].col_name, prefix, False, False)
    else:
        selec, hist_size = histogram_selectivity(rel_sta, col_sta, operator, prefix)

        if hist_size < 100:
            if pstatus == PatternPrefixStatus.PATTERN_PREFIX_PARTIAL:
                prefixsel = prefix_selectivity(rel_sta, col_sta, prefix)
            else:
                prefixsel = 1.0
            heursel = prefixsel * rest_selec
            if selec < 0:
                selec = heursel
            else:
                hist_weight = hist_size / 100.0
                selec = selec * hist_weight + heursel * (1.0 - hist_weight)
    
        if selec < 0.0001:
            selec = 0.0001
        elif selec > 0.9999:
            selec = 0.9999

        # 待检验是否可以直接调用
        mcv_selec, sumcommon = mcv_selectivity(rel_sta, col_sta, operator, prefix, True)
        
        selec *= 1.0 - nullfrac - sumcommon
        selec += mcv_selec
        result = selec

    if negate == True:
        result = 1.0 - result - nullfrac
    
    result = clamp_probability(result)
    return result

def likesel(root, rinfo):
    return patternsel(root, rinfo, PatternType.PATTERN_TYPE_LIKE, False)
