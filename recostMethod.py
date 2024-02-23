import math
from recostNode import *
import estimateCost as ec
import estimateRow as er
from util import *

def hashjoin_info(root: HashJoinNode, left_node: DerivedPlanNode, right_node: DerivedPlanNode):
    # ! 基数估计部分
    root.rows = er.calc_joinrel_size_estimate(root, left_node, right_node)

    # ! 代价估计部分
    
    # * init cost
    # startup_cost: 左表的startup_cost + 右表的total_cost(构建哈希表需要得到整个右表)
    #               + 右表构建哈希表的时间 + 右表哈希表分批构建的时间
    num_hashclauses = root.hashclauses.len()
    
    # cost of source data
    startup_cost = left_node.startup_cost       # 
    run_cost = left_node.total_cost - left_node.startup_cost
    startup_cost += right_node.total_cost
    
    # 构建哈希表并探查阶段:
    # 哈希处理: 使用DefaultOSCost.CPU_OPERATOR_COST.value来表示一次哈希需要的代价，然后再乘上num_hashclauses(哈希的次数)
    # 插入哈希表: 使用DefaultOSCost.CPU_TUPLE_COST.value来表示插入哈希表的代价
    # 右表对于每一列都需要哈希处理并插入哈希表，左表只需要哈希处理，不需要插入哈希表
    startup_cost += (DefaultOSCost.CPU_OPERATOR_COST.value * num_hashclauses + DefaultOSCost.CPU_TUPLE_COST.value) * right_node.rows
    run_cost += DefaultOSCost.CPU_OPERATOR_COST.value * num_hashclauses * left_node.rows

    # 分批处理需要的额外代价
    space_allowed, numbuckets, numbatches = ExecChooseHashTableSize(right_node.rows, right_node.width)
    root.numbatches = numbatches
    if numbatches > 1:
        left_pages = page_size(left_node.rows, left_node.width)
        right_pages = page_size(right_node.rows, right_node.width)

        startup_cost += DefaultOSCost.SEQ_PAGE_COST.value * right_pages
        run_cost += DefaultOSCost.SEQ_PAGE_COST.value * (right_pages + 2 * left_pages)
    
    # root.startup_cost = startup_cost
    # root.total_cost = startup_cost + run_cost

    # * final cost
    virtualbuckets = numbuckets * numbatches
    innerbucketsize = root.innerbucketsize
    innermcvrfreq = root.innermcvfreq

    # 如果右表的mcv太大,导致放不下DefaultOSSize.WORK_MEM.value, 则禁止使用hash
    # 一开始可以使用,但可能在后面发生数据偏移之后就不能使用了
    # ???: 如果是用于模型训练的话需要这样处理吗?
    if relation_byte_size(clamp_row_est(right_node.row * innermcvrfreq), 
                               right_node.width) > (DefaultOSSize.WORK_MEM.value * 1024):
        startup_cost += DefaultOSCost.DISABLE_COST.value

    # 计算哈希条件表达式的代价
    if root.jointype == "SEMI" or \
        root.jointype == "ANTI" or \
        root.inner_unique == True:
        # * 只要匹配一次的话, 进行哈希连接判断的次数会少很多
        outer_matched_rows = round(left_node.rows * root.semifactors_outer_match_frac)
        # 成功匹配到的(桶内)平均位置
        inner_scan_frac = 2.0 / (root.semifactors_match_count + 1.0)

        startup_cost += root.hash_qual_cost.startup
        # 成功匹配到的tuple, 进行哈希连接表达式计算的代价
        run_cost += root.hash_qual_cost.per_tuple * outer_matched_rows * \
            clamp_row_est(right_node.rows * innerbucketsize * inner_scan_frac) * 0.5
        
        # 未能成功匹配到的tuple, 进行哈希连接表达式计算的代价
        # 分为两部分: 
        # 1. 假设所有未匹配到的tuple都与对应bucket内的元组进行了哈希连接的检测
        # 2. 乘 0.05, 修正"没有对应的bucket,不需要进行哈希连接的检测"和"检测到一半就不符合匹配条件"两种情况
        run_cost += root.hash_qual_cost.per_tuple * \
            (left_node.rows - outer_matched_rows) * \
            clamp_row_est(right_node.rows / virtualbuckets) * 0.05
        # 计算通过哈希条件判断的元组数
        if root.jointype == "ANTI":
            hashjointuples = left_node.rows - outer_matched_rows
        else:
            hashjointuples = outer_matched_rows
    else:
        startup_cost += root.hash_qual_cost.startup
        run_cost += root.hash_qual_cost.per_tuple * left_node.rows * \
                    round(right_node.rows * innerbucketsize)
        # 计算通过哈希条件判断的元组数
        hashjointuples = clamp_row_est(root.approx_selec * left_node.rows * right_node.rows) 
    
    # 计算哈希连接之外的约束条件的检测代价
    startup_cost += root.qp_qual_cost.startup
    cpu_per_tuple = DefaultOSCost.CPU_TUPLE_COST.value + root.qp_qual_cost.per_tuple
    run_cost += cpu_per_tuple * hashjointuples

    # 投影最终结果的代价
    startup_cost += root.proj_cost.startup
    run_cost += root.proj_cost.per_tuple * root.rows

    # 保存当前节点的结果
    root.startup_cost = startup_cost
    root.total_cost = startup_cost + run_cost

    

def mergejoin_info(root: MergeJoinNode, left_node: DerivedPlanNode, right_node: DerivedPlanNode):
    # ! 基数估计
    root.rows = er.calc_joinrel_size_estimate(root, left_node, right_node)
    
    # ! 代价估计
    # 由于这是一颗已经建立完毕的查询树因此不需要考虑儿子节点无序的问题

    # * initial cost: 如果不是外连接,我们会在遍历完一个表的时候直接停止
    # startup_cost: 左右表的startup_cost + 左右表第一次匹配成功的代价
    # run_cost: 左右表第一次匹配成功到不可能产生新配对的代价

    # 计算左右表第一个匹配的位置
    # 只和表格的第一个约束条件和outer_pathkey有关, 可以持久化存储, 在节点初始化的时候进行设置
    outerstartsel = root.outerstartsel
    outerendsel = root.outerendsel
    innerstartsel = root.innerendsel
    innerendsel = root.innerendsel
    outer_skip_rows = round(left_node.rows * outerstartsel)
    inner_skip_rows = round(right_node.rows * innerstartsel)
    outer_rows = clamp_row_est(left_node.rows * outerendsel)
    inner_rows = clamp_row_est(right_node.rows * innerendsel)

    # 原本的实现有进行检测
    assert outer_skip_rows <= outer_rows, "mergejoin: outer_skip_rows too big"
    assert inner_skip_rows <= inner_rows, "mergejoin: inner_skip_rows too big"


    # 计算左右表的代价
    startup_cost = left_node.startup_cost + right_node.startup_cost
    startup_cost += (left_node.total_cost - left_node.startup_cost) * outerstartsel \
        + (right_node.total_cost - right_node.startup_cost) * innerstartsel
    run_cost += (left_node.total_cost - left_node.startup_cost) * (outerendsel - outerstartsel) \
        + (right_node.total_cost - right_node.startup_cost) * (innerendsel - innerstartsel)

    # * final cost
    

    mergejointuples = clamp_row_est(root.approx_selec * left_node.rows * right_node.rows)

    # ** mergejoin中如果左表有重复值,指向右表的指针可能需要反复扫描
    # 这里涉及重复扫描的时候,内表是否materialize的问题, 需要进行仔细的判断
    # 但是对于一颗确定的搜索树,这是否有必要呢?
    # !!原本是判断是否为uniquepath,这里直接使用节点类型判断
    if left_node.node_type == "Unique" or root.skip_mark_restore == True:
        rescannedtuples = 0
    else:
        rescannedtuples = max(mergejointuples - right_node.rows, 0)
    # 内表的每一个tuple平均会被重复扫rescanratio次
    rescanratio = 1.0 + (rescannedtuples / right_node.rows)
    # 计算重复扫的代价, 为内表的运行代价 * 重复扫描的次数
    bare_inner_cost = (right_node.total_cost - right_node.startup_cost) * rescanratio

    # 似乎是使用material处理之后, 重复扫描的代价不是重新运行右表,而是将其material化之后
    # 使用DefaultOSCost.CPU_OPERATOR_COST.value * right_node.rows * rescanratio来计算
    mat_inner_cost = (right_node.total_cost - right_node.startup_cost) + \
        DefaultOSCost.CPU_OPERATOR_COST.value * right_node.rows * rescanratio

    # ** inner table materialize的花费
    # if root.skip_mark_restore == True:
    #     root.materialize_inner = False
    
    # # enable_material这个标志似乎没有必要设置
    # # 这里将原本存在的enable_material的判断直接去掉
    # elif mat_inner_cost < bare_inner_cost: 
    #     root.materialize_inner = True

    # # 如果右表没有排好序,并且不支持mark/restore, 还是需要materialize_inner
    # elif root.innersortkeys == None and cs.ExecSupportsMarkRestore(right_node) == False:
    #     root.materialize_inner = True
    
    # # 
    # elif (root.innersortkeys != None 
    #       and relation_byte_size(right_node.rows, right_node.width) > (DefaultOSSize.WORK_MEM.value * 1024)):
    #     root.materialize_inner = True
    # else:
    #     root.materialize_inner = False
    
    # if root.materialize_inner:
    #     run_cost += mat_inner_cost
    # else:
    #     run_cost += bare_inner_cost
    # 直接判断inner table是不是materialize节点即可
    if right_node.node_type == "Materialize":
        run_cost += mat_inner_cost
    else :
        run_cost += bare_inner_cost
    
    # 计算连接条件表达式的代价
    startup_cost += root.merge_qual_cost.startup
    startup_cost += root.merge_qual_cost.per_tuple * \
        (outer_skip_rows + inner_skip_rows * rescanratio)
    run_cost += root.merge_qual_cost.per_tuple * \
        ((left_node.rows - outer_skip_rows) + \
         (right_node.rows - inner_skip_rows) * rescanratio)
    
    # 计算连接之外的约束条件的检测代价
    startup_cost += root.qp_qual_cost.startup
    cpu_per_tuple = DefaultOSCost.CPU_TUPLE_COST.value + root.qp_qual_cost.per_tuple
    run_cost += cpu_per_tuple * mergejointuples

    # 投影最终结果的代价
    startup_cost += root.proj_cost.startup
    run_cost += root.proj_cost.per_tuple * root.rows

    root.startup_cost = startup_cost
    root.total_cost = startup_cost + run_cost



def nestloop_info(root: NestLoopNode, left_node: DerivedPlanNode, right_node: DerivedPlanNode):
    # ! 基数估计
    root.rows = er.calc_joinrel_size_estimate(root, left_node, right_node)
    
    # ! 代价估计
    # * initial cost:
    # 重复扫描右表的代价提前计算出来
    inner_rescan_start_cost, inner_rescan_total_cost = ec.cost_rescan(right_node)
    
    # 计算左右表的代价
    startup_cost = left_node.startup_cost + right_node.startup_cost
    run_cost = left_node.total_cost - left_node.startup_cost
    if left_node.rows > 1:
        run_cost += (left_node.rows - 1) * inner_rescan_start_cost
    
    inner_run_cost = right_node.total_cost - right_node.startup_cost
    inner_rescan_run_cost = inner_rescan_total_cost - inner_rescan_start_cost

    # 当符合以下条件的时候不需要rescan
    if root.jointype == "SEMI" or \
        root.jointype == "ANTI" or \
        root.inner_unique == True:
        # 没有额外的代价,直接pass,具体的扫描次数在下面的final cost部分再进行计算
        pass
    else:
        run_cost += inner_run_cost
        if left_node.rows > 1:
            run_cost += (left_node.rows - 1) * inner_rescan_run_cost

    # * final cost
    if root.jointype == "SEMI" or \
        root.jointype == "ANTI" or \
        root.inner_unique == True:
        # 计算左表匹配以及不匹配的数量
        outer_matched_rows = round(left_node.rows * root.semifactors_outer_match_frac)
        outer_unmatched_rows = left_node.rows - outer_matched_rows
        # ? 这个变量的含义是什么
        # 成功匹配到的平均位置
        inner_scan_frac = 2.0 / (root.semifactors_match_count + 1.0)
        
        # 需要进行连接条件检测的对数
        ntuples = outer_matched_rows * right_node.rows * inner_scan_frac

        # 如果有index,那么代价会少很多
        if has_indexed_join_quals(root, right_node) == True:
            run_cost += inner_run_cost * inner_scan_frac
            if outer_matched_rows > 1:
                run_cost += (outer_matched_rows - 1) * inner_rescan_run_cost * inner_scan_frac
            #
            run_cost += outer_unmatched_rows * inner_rescan_run_cost / right_node.rows
        else:
            # 加上未匹配的tuple(会扫描整个右表)而产生的检测次数
            ntuples += outer_unmatched_rows * right_node.rows
            run_cost += inner_run_cost
            if outer_unmatched_rows >= 1:
                outer_unmatched_rows -= 1
            else:
                outer_matched_rows -= 1
            # 对于有匹配的tuple,会扫描到第差不多inner_scan_frac的时候停止
            if outer_matched_rows > 0:
                run_cost += outer_matched_rows * inner_rescan_run_cost * inner_scan_frac
            # 对于没有匹配的tuple,会扫描整个右表
            if outer_unmatched_rows > 0:
                run_cost += outer_unmatched_rows * inner_rescan_run_cost
    else:
        # 需要进行连接条件检测的对数: 整个扫描
        ntuples = left_node.rows * right_node.rows

    # 连接条件以及其他约束条件的检测代价
    startup_cost += root.qp_qual_cost.startup
    cpu_per_tuple = DefaultOSCost.CPU_TUPLE_COST.value + root.qp_qual_cost.per_tuple
    run_cost += cpu_per_tuple * ntuples

    # 投影最终结果的代价
    startup_cost += root.proj_cost.startup
    run_cost += root.proj_cost.per_tuple * root.rows

    root.startup_cost = startup_cost
    root.total_cost = startup_cost + run_cost

def unique_info(root: UniqueNode, son: DerivedPlanNode):
    # ! 基数估计:
    root.rows = estimate_num_groups(root, son)

    # ! 代价估计
    # unique操作的儿子节点一定是一个sort节点
    # 判断的方法是: 保存上一个行的信息,然后与当前行做相等判断,如果相等则不输出
    # 如果不相等则更新上一个行为当前行,然后更新当前行为下一行
    # startup_cost直接取儿子节点的startup_cost
    # run_cost需要额外计算判断的代价,即当前行与上一行的相等判断,一次判断的最高代价为
    # DefaultOSCost.CPU_OPERATOR_COST.value * numCols

    # 获得unique的列数, 源码的实现如下, 将全部的信息放在了sjinfo中
    # numCols = list_length(sjinfo->semi_rhs_exprs)
    numCols = root.num_cols
    root.startup_cost = son.startup_cost
    root.total_cost = son.total_cost
    # 往高了估计(即判断到最后一列),一次判断的代价为: DefaultOSCost.CPU_OPERATOR_COST.value * numCols
    root.total_cost += son.rows * DefaultOSCost.CPU_OPERATOR_COST.value * numCols

def gathermerge_info(root: GatherMergeNode, son: DerivedPlanNode):
    # gathermerge的儿子节点有很多个,但都是同一种类型
    # ! 基数估计
    root.rows = son.rows * root.num_workers

    # ! 代价估计
    # 儿子节点是并行执行的,所以只需要计算一次startup_cost和total_cost
    startup_cost = son.startup_cost
    run_cost = son.total_cost - son.startup_cost

    N = root.num_workers + 1
    logN = math.log2(N)
    # 每一次比较的代价
    comparison_cost = 2.0 * DefaultOSCost.CPU_OPERATOR_COST.value
    # 创建堆的花费
    startup_cost += comparison_cost * N * logN
    # merge的花费
    run_cost += root.rows * comparison_cost * logN
    # 堆管理的成本
    run_cost += DefaultOSCost.CPU_OPERATOR_COST.value * root.rows

    # 并行的额外成本
    # 由于gather_merge的有序性, 要求每一个worker都有tuple时才能进行merge
    # 所以将运行的成本调高了5%
    startup_cost += DefaultOSCost.PARALLEL_SETUP_COST.value
    run_cost += DefaultOSCost.PARALLEL_TUPLE_COST.value * root.rows * 1.05

    root.startup_cost = startup_cost
    root.total_cost = startup_cost + run_cost