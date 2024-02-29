from enum import Enum
from typing import TypeVar
import Recost_tmp.estimateCost as ecost
import Recost_tmp.estimateRow as erow
import Recost_tmp.util as util
from Recost_tmp.PlanNode.planNodeAPI import *
from Recost_tmp.PlanNode.filterNode import parse_filter
from Recost_tmp.recostMethod import *


class Node(PlanNodeInterface):
    def __init__(self, json_dict):
        super().__init__()
        # get from explain
        for nt in NodeType:
            if nt.value == json_dict['Node Type']:
                self.node_type = nt
                break
        self.startup_cost = json_dict["Startup Cost"]
        self.total_cost = json_dict["Total Cost"]
        self.rows = json_dict["Plan Rows"]
        self.width = json_dict["Plan Width"]
        self.actual_startup_time = json_dict["Actual Startup Time"]
        self.actual_total_time = json_dict["Actual Total Time"]
        self.actual_rows = json_dict["Actual Rows"]
        self.output = json_dict["Output"]       #??? 似乎没有
        self.relation_name = json_dict.get("Relation Name", None)
        
        # init in initialization of derived class
        self.filter_src = None          # 过滤条件(字符串格式)
        self.filter = []              # 过滤条件(restrictInfo格式)
        self.simple_rel_array = []      # 所有涉及的简单表格
        self.restrict_list = []       # 所有约束条件(restrictInfo格式)
        self.qp_qual_cost = None        # 全部约束条件的cost
        # init out-of-class
        self.children = []
        self.recost_fun = None
    
    def getFilter(self, filter_str):
        self.filter_src = filter_str
        if self.filter_src != None:
            self.filter = [parse_filter(self.filter_src)]

    def getSimpleRelArray(self, json_dict):
        # simple_rel_array
        for child in self.children:
            self.simple_rel_array.extend(child.simple_rel_array)
        if self.relation_name != None:
            self.simple_rel_array.append(self.relation_name)
    

    def extendRestrictList(self, restrict_list):
        if restrict_list != None:
            self.restrict_list.extend(restrict_list)

    def calQualCost(self):
        self.qp_qual_cost = ecost.cost_qual_eval(self.restrict_list, self)

    def with_alias(self, alias):
        self.table_alias = alias
        return self

    def get_table_id(self, with_alias=True, alias_only=False):
        """Table id for disambiguation."""
        if with_alias and self.table_alias:
            if alias_only:
                return self.table_alias
            return self.table_name + ' AS ' + self.table_alias
        assert self.table_name is not None
        return self.table_name

class JoinNode(Node, JoinNodeInterface):
    def __init__(self, json_dict):
        JoinNodeInterface.__init__(self)
        Node.__init__(self, json_dict)
        # get from explain
        for jt in JoinType:
            if jt.value == json_dict["Join Type"]:
                self.join_type = jt
                break
        self.inner_unique = json_dict["Inner Unique"]
        self.rows_removed_by_filter = json_dict.get("Rows Removed by Filter", None)

        # init after son init
        self.semifactors_outer_match_frac = None
        self.semifactors_match_count = None
        self.join_filter_src = None
        self.join_filter = []
        self.fkselec = 1.0
        self.jselec = 1.0
        self.pselec = 1.0       
        self.approx_selec = 1.0 #用于大致估计行数
        self.proj_cost = None

    def calSemifactors(self):
        # semifactors
        if self.join_type == JoinType.ANTI or self.join_type == JoinType.SEMI or self.inner_unique == True:
            joinquals = []
            if util.IS_OUTER_JOIN(self.join_type):
                joinquals.extend(self.join_filter)
            else:
                joinquals.extend(self.restrict_list)
            # TODO: 这里如何传入simple_rel_array参数比较合适?
            self.semifactors_outer_match_frac = erow.clauselist_selectivity(self, joinquals, join_type=JoinType.ANTI if self.join_type == JoinType.ANTI else JoinType.SEMI)
            self.semifactors_match_count = max(1.0, 
                erow.clauselist_selectivity(self, joinquals, join_type=JoinType.INNER) / self.semifactors_outer_match_frac * self.children[1].rows)

    def getJoinFilter(self, join_filter_src):
        self.join_filter_src = join_filter_src
        if self.join_filter_src != None:
            self.join_filter = [parse_filter(self.join_filter_src)]

    def extendJoinFilter(self, join_filter):
        if join_filter != None:
            self.join_filter.extend(join_filter)

    def getSelec(self):
        self.fkselec, self.jselec, self.pselec = erow.get_join_selectivity(self, self.children[0], self.children[1])

    def getApproxSelec(self, restrict_list):
        self.approx_selec = 1.0
        for rt in restrict_list:
            self.approx_selec *= erow.clause_selectivity(self, rt)

    def getProjCost(self):
        # TODO: proj_cost:
        self.proj_cost = []


class ScanNode(Node, ScanNodeInterface):
    def __init__(self, json_dict):
        ScanNodeInterface.__init__(self)
        Node.__init__(self, json_dict)

class HashJoinNode(JoinNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        # restrict list
        if json_dict.get("Filter", None) != None:
            self.getFilter(json_dict["Filter"])
            self.extendRestrictList(self.filter)
        if json_dict.get("Join Filter", None) != None:
            self.getJoinFilter(json_dict["Join Filter"])
            self.extendRestrictList(self.join_filter)
        self.hash_cond_src = json_dict["Hash Cond"]
        self.hash_cond = [parse_filter(self.hash_cond_src)]
        self.extendRestrictList(self.hash_cond)
        self.extendJoinFilter(self.hash_cond)

        # qual cost
        self.calQualCost()
        self.hash_qual_cost = ecost.cost_qual_eval(self.hash_cond, self)
        self.qp_qual_cost.startup -= self.hash_qual_cost.startup
        self.qp_qual_cost.per_tuple -= self.hash_qual_cost.per_tuple



        # projcost
        self.getProjCost()

        # get from recost_fun
        self.numbatches = None


    # 数据发生偏移的时候调用
    def updateWhenDataChange(self):
        self.innerbucketsize, self.innermcvfreq = util.estimate_hash_bucket_stats()

    def initAfterSonInit(self):
        # selectivity
        self.getSelec()
        self.getApproxSelec(self.hash_cond)
        self.calSemifactors()
        # get from static
        self.innerbucketsize, self.innermcvfreq = util.estimate_hash_bucket_stats()

class MergeJoinNode(JoinNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        # restrict list
        if json_dict.get("Filter", None) != None:
            self.getFilter(json_dict["Filter"])
            self.extendRestrictList(self.filter)
        if json_dict.get("Join Filter", None) != None:
            self.getJoinFilter(json_dict["Join Filter"])
            self.extendRestrictList(self.join_filter)
        self.merge_cond_src = json_dict["Merge Cond"]
        self.merge_cond = [parse_filter(self.merge_cond_src)]
        self.extendRestrictList(self.merge_cond)
        self.extendJoinFilter(self.merge_cond)

        # qual_cost
        self.calQualCost()
        self.merge_qual_cost = ecost.cost_qual_eval(self.merge_cond, self)
        self.qp_qual_cost.startup -= self.merge_qual_cost.startup
        self.qp_qual_cost.per_tuple -= self.merge_qual_cost.per_tuple

        
        # projcost
        self.getProjCost()

        # skip_mark_restore表示是否跳过重复匹配
        self.skip_mark_restore = False
        if ((self.join_type == JoinType.SEMI or
            self.join_type == JoinType.ANTI or
            self.inner_unique == True) and 
            (len(self.restrict_list) == len(self.merge_cond))):   # 所有的约束条件都是可以归并的(即join filter应该为空), 并且满足一个outer tuple只会和一个inner tuple匹配
            self.skip_mark_restore = True

        # startsel, endsel
        self.outerstartsel, self.outerendsel, self.innerstartsel, self.innerendsel = erow.mergejoinscansel(self)

    def initAfterSonInit(self):
        self.getSimpleRelArray()
        self.calSemifactors()
        # selectivity
        self.getSelec()
        self.getApproxSelec(self.merge_cond)


class NestLoopNode(JoinNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        if json_dict.get("Filter", None) != None:
            self.getFilter(json_dict["Filter"])
            self.extendRestrictList(self.filter)
        if json_dict.get("Join Filter", None) != None:
            self.getJoinFilter(json_dict["Join Filter"])
            self.extendRestrictList(self.join_filter)
        
        # qual_cost
        self.calQualCost()

        
        # projcost
        self.getProjCost()

        # get after processing
        self.join_qual_cost = None
    
    def initAfterSonInit(self):
        self.getSimpleRelArray()
        self.calSemifactors()
        # selectivity
        self.getSelec()

class SeqScanNode(ScanNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    def initAfterSonInit(self):
        pass

class IndexScanNode(ScanNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    def initAfterSonInit(self):
        pass
class IndexOnlyScanNode(ScanNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    def initAfterSonInit(self):
        pass
class MaterialNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    def initAfterSonInit(self):
        pass
class SortNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    def initAfterSonInit(self):
        pass
class UniqueNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        self.num_cols = None
    def initAfterSonInit(self):
        pass
class GatherMergeNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        self.num_workers = None
    def initAfterSonInit(self):
        pass
class HashNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        self.num_workers = None
    def initAfterSonInit(self):
        pass


def nodeFactory(json_dict):
    for nt in NodeType:
        if nt.value == json_dict['Node Type']:
            node_type = nt
            break
    if node_type == NodeType.NESTED_LOOP:
        curr_node = NestLoopNode(json_dict)
        curr_node.recost_fun = nestloop_info
    elif node_type == NodeType.HASH_JOIN:
        curr_node = HashJoinNode(json_dict)
        curr_node.recost_fun = hashjoin_info
    elif node_type == NodeType.MERGE_JOIN:
        curr_node = MergeJoinNode(json_dict)
        curr_node.recost_fun = mergejoin_info
    elif node_type == NodeType.SEQ_SCAN:
        curr_node = SeqScanNode(json_dict)
        curr_node.recost_fun = seqscan_info
    elif node_type == NodeType.HASH:
        curr_node = HashNode(json_dict)
        curr_node.recost_fun = hash_info
        
    return curr_node