from util import *

def calc_joinrel_size_estimate(joinrel, left_node, right_node):
    jointype = joinrel.jointype
    # 如果还没有获得过当前joinrel的selec, 进行一下初始化
    if joinrel.fkselec == None:
        get_foreign_key_join_selectivity(joinrel, left_node, right_node)
    if joinrel.jselec == None:
        get_join_and_push_selectivity(joinrel, left_node, right_node)
     
    fkselec = joinrel.fkselec
    jselec = joinrel.jselec
    pselec = joinrel.pselec
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

# TODO: 根据joinrel的restrictlist得到joinrel的fkselec
def get_foreign_key_join_selectivity(joinrel, left_node, right_node):
    pass

# TODO: 根据joinrel的restrictlist得到joinrel的jselec和pselec
def get_join_and_push_selectivity(joinrel, left_node, right_noed):
    pass

# TODO: 计算给定表格符合给定约束条件的大概元组数量
def approx_tuple_count(root, clauses):
    pass
