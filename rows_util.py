import copy
from typing import List
from util import *

def calc_joinrel_size_estimate(root, left_node, right_node):
    jointype = root.jointype
    # 如果还没有获得过当前root的selec, 进行一下初始化
    if root.fkselec == None:
        get_foreign_key_join_selectivity(root, left_node, right_node)
    if root.jselec == None:
        get_join_and_push_selectivity(root, left_node, right_node)
     
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
    elif jointype == "JOIN_SEMI":
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

def estimate_num_groups(root, son):
    pass

# TODO: 根据root的restrictlist得到root的fkselec
def get_foreign_key_join_selectivity(root, left_node, right_node):
    # 遍历所有的fk
    for fkinfo in root.fkey_list:
        # 外键的con_relid是referencing的table, ref_relid是referenced的table
        if Bitmapset.bms_is_member(fkinfo.con_relid, left_node.relids) and \
            Bitmapset.bms_is_member(fkinfo.ref_relid, right_node.relids):
            ref_is_outer = False
        elif Bitmapset.bms_is_member(fkinfo.ref_relid, left_node.relids) and \
            Bitmapset.bms_is_member(fkinfo.con_relid, right_node.relids):
            ref_is_outer = True
        else:
            continue
        # 当是JOIN_SEMI或JOIN_ANTI连接类型时
        # 如果outer是被引用的表, 那么这时候太难计算了,直接跳过
        # 或如果右表太多,无法顾及其他约束条件时,还是太难计算了,直接跳过
        if ((root.jointype == "JOIN_SEMI" or root.jointype == "JOIN_ANTI") and
            (ref_is_outer == True or Bitmapset.bms_membership(right_node.relids) 
             != "BMS_SINGLETON")):
            continue
        
        # 剩下的好像是一些restrictlist的更新操作
        #? 用于防止重复计算吗
        # if worklist == root.restrictlist:
        #     worklist = copy.copy(worklist)
        # removedlist = None
        # prev = None
        # for rinfo in worklist:


        #
        if root.jointype == "JOIN_SEMI" or \
            root.jointype == "JOIN_ANTI":
            # ? 为什么这里使用rows/tuples   
            ref_rel = find_base_rel(root, fkinfo.ref_relid)
            fkselec *= ref_rel.rows / max(ref_rel.tuples, 1.0)
        else:
            ref_rel = find_base_rel(root, fkinfo.ref_relid)
            fkselec *= 1.0 / max(ref_rel.tuples, 1.0)
    
    # 关于restrictlist的更新操作
    #/ *restrictlist = worklist

    return fkselec

# TODO: 根据root的restrictlist得到root的jselec和pselec
def get_join_and_push_selectivity(root, left_node, right_noed):
    pass

# TODO: 计算给定表格符合给定约束条件的大概元组数量
# 实现逻辑: 乘上所有的约束条件,得到selec,然后直接与笛卡尔积结果相乘,得到大致估计结果
# TODO: 这里的selec是恒定不变的, 考虑作为root的一个变量进行持久化存储
def approx_tuple_count(root, clauses):
    selec = 1.0
    for qual in clauses:
        # pg的实现,还需要传入连接类型和special join info信息
        selec *= clause_selectivity(root, qual)
    return clamp_row_est(selec * root.children[0].rows * root.children[1].rows)

# TODO: 计算给定约束条件下的选择率
def clause_selectivity(root, clause: Clause):
    s1 = 0.5

    if clause == None:
        return s1
    
    # 根据不同类型的clause进行不同的判断
    # 这里原本的实现用到了很多指针的特性,代码复杂度较大
    # TODO: 考虑进行重构

def clauselist_selectivity(root, clauses: List[Clause]):
    pass