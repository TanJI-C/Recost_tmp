import math
from util import *


#TODO: 关于并行处理，暂时假设不存在
def ExecChooseHashTableSize(ntuples, tupwidth, useskew = False, try_combined_work_mem = False, parallel_workers = 0):
    tupsize = HJTUPLE_OVERHEAD + typealign(8, SizeofMinimalTupleHeader) + typealign(8, tupwidth)
    inner_rel_bytes = ntuples * tupsize

    hash_table_bytes = work_mem * 1024

    if try_combined_work_mem == True:
        hash_table_bytes = hash_table_bytes * parallel_workers
    
    space_allowed = hash_table_bytes

    if useskew == True:
        skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PRECENT // 100
        num_skew_mcvs = skew_table_bytes // (tupsize +\
                                             (8 * 8)) + \
                                             4 + SKEW_BUCKET_OVERHEAD
        if num_skew_mcvs > 0:
            hash_table_bytes -= skew_table_bytes
    else:
        num_skew_mcvs = 0

    max_pointers = min(space_allowed // HashJoinTupleSize, MaxAllocSize // HashJoinTupleSize)
    mppow2 = 1 << math.ceil(math.log2(max_pointers))
    if max_pointers == mppow2:
        max_pointers = mppow2 // 2
    max_pointers = min(max_pointers, INT_MAX // 2)

    dbuckets = min(math.ceil(ntuples / NTUP_PER_BUCKET), max_pointers)
    nbuckets = max(dbuckets, 1024)
    nbuckets = 1 << math.ceil(math.log2(nbuckets))

    bucket_bytes = HashJoinTupleSize * nbuckets
    if inner_rel_bytes + bucket_bytes > hash_table_bytes:
        if try_combined_work_mem == True:
            ExecChooseHashTableSize(ntuples, tupwidth, useskew,
                                    False, parallel_workers)
            return
        bucket_size = tupsize * NTUP_PER_BUCKET + HashJoinTupleSize
        lbuckets = 1 << math.ceil(math.log2(hash_table_bytes // bucket_size))
        lbuckets = min(lbuckets, max_pointers)
        nbuckets = int(lbuckets)
        nbuckets = 1 << math.ceil(math.log2(nbuckets))
        bucket_bytes = nbuckets * HashJoinTupleSize

        dbatch = math.ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes))
        dbatch = min(dbatch, max_pointers)
        minbatch = int(dbatch)
        nbatch = 2
        while nbatch < minbatch:
            nbatch = nbatch << 1
    
    return space_allowed, nbuckets, nbatch




# TODO: 计算给定约束条件下的代价
def cost_qual_eval(clauses, root: DerivedPlanNode):
    pass

# TODO: 计算重复扫描右表的代价，在nestloop中需要重复扫描右表
def cost_rescan(root, right_node):
    pass

# TODO: 判断当前路径是否支持mark或者restore
# TODO: 未完成
def ExecSupportsMarkRestore(root):
    pathtype = type(root)
    # * 这里需要其他节点类型的名字接口,后续可能需要进行修改
    if pathtype == IndexScanNode or \
        pathtype == IndexOnlyScanNode or \
        pathtype == MaterialNode or \
        pathtype == SortNode:
        return True
    # ? 自定义扫描不是很理解
    # TODO: 暂时假设其不存在
    # elif pathtype == CustomScanNode:
    #     pass
    # ? Result节点不是很理解
    # TODO: 暂时假设其不存在
    # elif pathtype == ResultNode:
    #     pass
    
    return False

# TODO
def has_indexed_join_quals(root, right_node):
    pass

