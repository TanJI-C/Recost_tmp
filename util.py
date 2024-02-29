import logging
from typing import List
from Recost_tmp.PlanNode.planNodeAPI import JoinType, NodeType
import math
import re

class DefaultVal:
    INT_MAX = 0x7fffffff
    DEFAULT_NUM_DISTINCT = 200

class DefaultOSCost:
    # base const
    CPU_OPERATOR_COST = 0.0025
    CPU_TUPLE_COST = 0.01
    SEQ_PAGE_COST = 1.0

    PARALLEL_TUPLE_COST = 0.1
    PARALLEL_SETUP_COST = 1000
    DISABLE_COST = DefaultVal.INT_MAX


class DefaultOSSize:
    HJTUPLE_OVERHEAD = 16 # typealign(8, sizeof (HashJoinTupleData))
    
    SIZE_OF_MINIMAL_TUPLE_HEADER = 32 
    SIZE_OF_HEAP_TUPLE_HEADER = 32      

    BLCKSZ = 8192
    WORK_MEM = 1024

    NTUP_PER_BUCKET = 1

    SKEW_WORK_MEM_PRECENT = 2
    SKEW_BUCKET_OVERHEAD = 16


    HASH_JOIN_TUPLE_SIZE = 8

    MAX_ALLOC_SIZE = 0x3fffffff


# base method
def typealign(alignment, length):
    return (length + (alignment - 1)) & ~(alignment - 1)

def clamp_row_est(rows):
    if rows <= 1.0:
        rows = 1.0
    else:
        rows = round(rows)
    return rows

def clamp_probability(selec):
    if selec < 0.0:
        selec = 0.0
    if selec > 1.0:
        selec = 1.0
    return selec

def elog(level, message, *args):
    if level == "ERROR":
        logging.error(message, *args)
    elif level == "WARNING":
        logging.warning(message, *args)
    elif level == "INFO":
        logging.info(message, *args)
    elif level == "DEBUG":
        logging.debug(message, *args)
    else:
        logging.error("Invalid log level: {}", level)
        
def find_base_rel(root, relid):
    if relid < len(root.simple_rel_array):
        rel = root.simple_rel_array[relid]
        if rel != None:
            return rel
    elog("ERROR", "no relation entry for relid %d", relid)
    return None

# TODO: 根据统计信息获取指定表格的tuples数
def get_tuples_num_of_rel(rel_name):
    return 10

def IS_OUTER_JOIN(join_type: JoinType):
    if join_type == JoinType.LEFT or join_type == JoinType.RIGHT or join_type == JoinType.FULL or join_type == JoinType.ANTI:
        return True
    return False



# 判断是否所有的连接谓词都被用于内部索引谓词
def has_indexed_join_quals(root, right_node):
    # !!原本的判断需要考虑参数化约束语句,判断起来比较复杂
    # !!考虑通过判断right_node的节点是不是index来进行判断
    node_type = right_node.node_type
    if node_type == NodeType.INDEX_SCAN or \
        node_type == NodeType.INDEX_ONLY_SCAN or \
        node_type == NodeType.BITMAP_HEAP_SCAN:
        return True
    return False

# TODO: 返回估计的bucketsize和mcvfreq
def estimate_hash_bucket_stats(root, vardata):
    pass


def ExecChooseHashTableSize(ntuples, tupwidth, useskew = False, try_combined_work_mem = False, parallel_workers = 0):
    tupsize = DefaultOSSize.HJTUPLE_OVERHEAD + typealign(8, DefaultOSSize.SIZE_OF_MINIMAL_TUPLE_HEADER) + typealign(8, tupwidth)
    inner_rel_bytes = ntuples * tupsize

    hash_table_bytes = DefaultOSSize.WORK_MEM * 1024

    if try_combined_work_mem == True: #??? 多个哈希全部在同一个内存中处理
        hash_table_bytes = hash_table_bytes * parallel_workers
    
    space_allowed = hash_table_bytes
    # hash table需要给skew batch始终预留一部分空间
    if useskew == True:
        skew_table_bytes = hash_table_bytes * DefaultOSSize.SKEW_WORK_MEM_PRECENT // 100
        num_skew_mcvs = skew_table_bytes // (tupsize +\
                                             ((8 * 8)) + \
                                             4 + DefaultOSSize.SKEW_BUCKET_OVERHEAD)
        if num_skew_mcvs > 0:
            hash_table_bytes -= skew_table_bytes
    else:
        num_skew_mcvs = 0

    # 算bucket
    max_pointers = min(space_allowed // DefaultOSSize.HASH_JOIN_TUPLE_SIZE, DefaultOSSize.MAX_ALLOC_SIZE // DefaultOSSize.HASH_JOIN_TUPLE_SIZE)
    mppow2 = 1 << math.ceil(math.log2(max_pointers))
    if max_pointers != mppow2:
        max_pointers = mppow2 // 2
    max_pointers = min(max_pointers, DefaultVal.INT_MAX // 2)

    dbuckets = min(math.ceil(ntuples / DefaultOSSize.NTUP_PER_BUCKET), max_pointers)
    nbuckets = max(dbuckets, 1024)
    nbuckets = 1 << math.ceil(math.log2(nbuckets))

    nbatch = 1
    bucket_bytes = DefaultOSSize.HASH_JOIN_TUPLE_SIZE * nbuckets
    # 一批处理不了,考虑分批次,并重新计算bucket和batch
    if inner_rel_bytes + bucket_bytes > hash_table_bytes:
        if try_combined_work_mem == True:       # ??? 并行处理不能分批次吗
            return ExecChooseHashTableSize(ntuples, tupwidth, useskew,
                                    False, parallel_workers)
        bucket_size = tupsize * DefaultOSSize.NTUP_PER_BUCKET + DefaultOSSize.HASH_JOIN_TUPLE_SIZE
        lbuckets = 1 << math.ceil(math.log2(hash_table_bytes // bucket_size))
        lbuckets = min(lbuckets, max_pointers)
        nbuckets = int(lbuckets)
        nbuckets = 1 << math.ceil(math.log2(nbuckets))
        bucket_bytes = nbuckets * DefaultOSSize.HASH_JOIN_TUPLE_SIZE

        dbatch = math.ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes))
        dbatch = min(dbatch, max_pointers)
        minbatch = int(dbatch)
        nbatch = 2
        while nbatch < minbatch:
            nbatch = nbatch << 1
    
    assert nbuckets > 0, "ExecChooseHashTableSize: nbuckets <= 0"
    assert nbatch > 0, "ExecChooseHashTableSize: nbatch <= 0"

    return space_allowed, nbuckets, nbatch


# 给定表格的元组数量和元组宽度, 返回表格的字节大小
def relation_byte_size(ntuples, tupwidth):
    return ntuples * (typealign(8, tupwidth) + typealign(8, DefaultOSSize.SIZE_OF_HEAP_TUPLE_HEADER))
# 给定tuples数量和tuple宽度, 返回页数
def page_size(ntuples, tupwidth):
    return math.ceil(relation_byte_size(ntuples, tupwidth)) / DefaultOSSize.BLCKSZ


def estimate_num_groups(root, son):
    pass

# T根据root的restrictlist得到root的fkselec
# !! 基本原理是通过判断连接条件的右表是否是foreign key,然后累乘选择性
# !! 简化原本的操作,直接判断连接条件使用的谓词是否是主键,然后直接得到结果
# def get_foreign_key_join_selectivity(root, left_node, right_node):
#     # 遍历所有的fk
#     fkselec = 1.0
#     for fkinfo in root.fkey_list:
#         # 外键的con_relid是referencing的table, ref_relid是referenced的table
#         if Bitmapset.bms_is_member(fkinfo.con_relid, left_node.relids) and \
#             Bitmapset.bms_is_member(fkinfo.ref_relid, right_node.relids):
#             ref_is_outer = False
#         elif Bitmapset.bms_is_member(fkinfo.ref_relid, left_node.relids) and \
#             Bitmapset.bms_is_member(fkinfo.con_relid, right_node.relids):
#             ref_is_outer = True
#         else:
#             continue
#         # 当是JOIN_SEMI或JOIN_ANTI连接类型时
#         # 如果outer是被引用的表, 那么这时候太难计算了,直接跳过
#         # 或如果右表太多,无法顾及其他约束条件时,还是太难计算了,直接跳过
#         if ((root.jointype == "JOIN_SEMI" or root.jointype == "JOIN_ANTI") and
#             (ref_is_outer == True or Bitmapset.bms_membership(right_node.relids) 
#              != "BMS_SINGLETON")):
#             continue
        
#         # 剩下的好像是一些restrictlist的更新操作
#         #? 用于防止重复计算吗
#         # if worklist == root.restrictlist:
#         #     worklist = copy.copy(worklist)
#         # removedlist = None
#         # prev = None
#         # for rinfo in worklist:


#         #
#         if root.jointype == "JOIN_SEMI" or \
#             root.jointype == "JOIN_ANTI":
#             # ? 为什么这里使用rows/tuples   
#             ref_rel = find_base_rel(root, fkinfo.ref_relid)
#             fkselec *= ref_rel.rows / max(ref_rel.tuples, 1.0)
#         else:
#             ref_rel = find_base_rel(root, fkinfo.ref_relid)
#             fkselec *= 1.0 / max(ref_rel.tuples, 1.0)
    
#     # 关于restrictlist的更新操作
#     #/ *restrictlist = worklist

#     return fkselec


# TODO: 返回相反的符号
def get_negator(operator):
    pass

# TODO: 返回相反方向的符号
def get_commutator(operator):
    pass

# TODO: 返回二元运算符的结果, 目前需要实现<, <=, >, >=, =, <>六种比较运算符即可
# 比较复杂, 因为需要实现不同类型之间对同一个操作符的运算结果
# 源码中, 对于不同类型直接的运算比较是通过操作符族实现的
def get_result_of_operator(left, right, operator):
    pass


# TODO: 根据不同的类型返回值
def convert_to_scalar(constval, loval, hival, col_sta):
    pass

# 查找给定列的最小值和最大值, 并返回是否找到了
def get_actual_variable_range(col_sta):
    if col_sta.min == None or col_sta.max == None:
        return False, -1, -1
    return True, col_sta.minval, col_sta.maxval




def parse_expression(expression):
    # 去除多余空格, 默认给出的expression没有换行符
    expression = re.sub(r'\s+', ' ', expression.strip())
    # 先根据括号进行划分
    pattern = r'\((.*?)\)'