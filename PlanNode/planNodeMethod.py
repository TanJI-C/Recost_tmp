from enum import Enum
from typing import TypeVar
import Recost_tmp.estimateCost as ecost
import Recost_tmp.estimateRow as erow
import Recost_tmp.util as util
from Recost_tmp.PlanNode.planNodeAPI import *
from Recost_tmp.PlanNode.filterNode import parse_filter, Cost
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
        self.relation_alias = json_dict.get("Alias", None)
        
        # init in initialization of derived class
        self.filter_src = None          # 过滤条件(字符串格式)
        self.filter = []              # 过滤条件(restrictInfo格式)
        self.simple_rel_array = []      # 所有涉及的简单表格
        self.simple_rel_alias_dict = {}
        self.restrict_list = []       # 所有约束条件(restrictInfo格式)
        self.qp_qual_cost = Cost()        # 全部约束条件的cost
        # init out-of-class
        self.children = []

        # 第一次预估中，估计的行数就是其本身
        self.est_rows = self.rows
    
    def get_filter(self, filter_str):
        self.filter_src = filter_str
        if self.filter_src != None:
            self.filter = [parse_filter(self.filter_src)]
       

    def get_simple_rel_array(self):
        # simple_rel_array
        for child in self.children:
            self.simple_rel_array.extend(child.simple_rel_array)
            self.simple_rel_alias_dict.update(child.simple_rel_alias_dict)
        if self.relation_name != None:
            self.simple_rel_array.append(self.relation_name)
            self.simple_rel_alias_dict['omit'] = self.relation_name
            if self.relation_alias != None:
                self.simple_rel_alias_dict[self.relation_alias] = self.relation_name
        self.adjust_filter()

    def adjust_filter(self):
        for fi in self.filter:
            fi.replace_alias(self.simple_rel_alias_dict)
        for ri in self.restrict_list:
            ri.replace_alias(self.simple_rel_alias_dict)

    def extend_restrict_list(self, restrict_list):
        if restrict_list != None:
            self.restrict_list.extend(restrict_list)

    def cal_qual_cost(self):
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
    
    def init_after_son_init(self):
        self.get_simple_rel_array()

    def update_param(self, dict):
        for fi in self.filter:
            ri.updateParam(dict)
        for ri in self.restrict_list:
            fi.updateParam(dict)

    def to_dict(self):
        node_dict = {
            "node_type": self.node_type.value,
            "est_rows": self.est_rows,
            "est_startup_cost": self.est_startup_cost,
            "est_total_cost": self.est_total_cost,
            "rows": self.rows,
            "width": self.width,
            "actual_startup_time": self.actual_startup_time,
            "actual_total_time": self.actual_total_time,
            "actual_rows": self.actual_rows,
            "output": self.output,
            "relation_name": self.relation_name,
            "relation_alias": self.relation_alias,
            "filter_src": self.filter_src,
            "restrict_list": [ri.to_dict() for ri in self.restrict_list],
            # "restrict_list": self.restrict_list,
            "simple_rel_array": self.simple_rel_array,
            "simple_rel_alias_dict": self.simple_rel_alias_dict,
            "qp_qual_cost": [self.qp_qual_cost.startup, self.qp_qual_cost.per_tuple],
            "children": [chi.to_dict() for chi in self.children],
        }
        return node_dict
    
    def recost_fun(self):
        pass

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

    def cal_semifactors(self):
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
                erow.clauselist_selectivity(self, joinquals, join_type=JoinType.INNER) / self.semifactors_outer_match_frac * self.children[1].est_rows)

    def adjust_filter(self):
        Node.adjust_filter(self)
        for jf in self.join_filter:
            jf.replace_alias(self.simple_rel_alias_dict)


    def get_join_filter(self, join_filter_src):
        self.join_filter_src = join_filter_src
        if self.join_filter_src != None:
            self.join_filter = [parse_filter(self.join_filter_src)]

    def extend_join_filter(self, join_filter):
        if join_filter != None:
            self.join_filter.extend(join_filter)

    def get_selec(self):
        self.fkselec, self.jselec, self.pselec = erow.get_join_selectivity(self, self.children[0], self.children[1])

    def get_approx_selec(self, restrict_list):
        self.approx_selec = 1.0
        for rt in restrict_list:
            self.approx_selec *= erow.clause_selectivity(self, rt, self.join_type)

    def get_proj_cost(self):
        # TODO: proj_cost:
        self.proj_cost = Cost()

    def update_param(self, dict):
        Node.update_param(self, dict)
        for jf in self.join_filter:
            jf.updateParam(dict)
        self.cal_semifactors()
        self.get_selec()

    def to_dict(self):
        dict = super().to_dict()
        dict['fkselec'] = self.fkselec
        dict['jselec'] = self.jselec
        dict['pselec'] = self.pselec
        return dict
    

class ScanNode(Node, ScanNodeInterface):
    def __init__(self, json_dict):
        ScanNodeInterface.__init__(self)
        Node.__init__(self, json_dict)

class HashJoinNode(JoinNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        # restrict list
        if json_dict.get("Filter", None) != None:
            self.get_filter(json_dict["Filter"])
            self.extend_restrict_list(self.filter)
        if json_dict.get("Join Filter", None) != None:
            self.get_join_filter(json_dict["Join Filter"])
            self.extend_restrict_list(self.join_filter)
        self.hash_cond_src = json_dict["Hash Cond"]
        self.hash_cond = [parse_filter(self.hash_cond_src)]
        self.extend_restrict_list(self.hash_cond)
        self.extend_join_filter(self.hash_cond)

        # qual cost
        self.cal_qual_cost()
        self.hash_qual_cost = ecost.cost_qual_eval(self.hash_cond, self)
        self.qp_qual_cost.startup -= self.hash_qual_cost.startup
        self.qp_qual_cost.per_tuple -= self.hash_qual_cost.per_tuple



        # projcost
        self.get_proj_cost()

        # get from recost_fun
        self.numbatches = None


    def adjust_filter(self):
        super().adjust_filter()
        for hc in self.hash_cond:
            hc.replace_alias(self.simple_rel_alias_dict)

    # TODO: 数据发生偏移的时候调用

    def update_param(self, dict):
        super().update_param(dict)
        for hc in self.hash_cond:
            hc.updateParam(dict)
    
    def init_after_son_init(self):
        self.get_simple_rel_array()
        self.cal_semifactors()
        # selectivity
        self.get_selec()
        self.get_approx_selec(self.hash_cond)
        # get from static

    def recost_fun(self):
        hashjoin_info(self, self.children[0], self.children[1])
        

class MergeJoinNode(JoinNode):

    def __init__(self, json_dict):
        super().__init__(json_dict)
        # restrict list
        if json_dict.get("Filter", None) != None:
            self.get_filter(json_dict["Filter"])
            self.extend_restrict_list(self.filter)
        if json_dict.get("Join Filter", None) != None:
            self.get_join_filter(json_dict["Join Filter"])
            self.extend_restrict_list(self.join_filter)
        self.merge_cond_src = json_dict["Merge Cond"]
        self.merge_cond = [parse_filter(self.merge_cond_src)]
        self.extend_restrict_list(self.merge_cond)
        self.extend_join_filter(self.merge_cond)

        # qual_cost
        self.cal_qual_cost()
        self.merge_qual_cost = ecost.cost_qual_eval(self.merge_cond, self)
        self.qp_qual_cost.startup -= self.merge_qual_cost.startup
        self.qp_qual_cost.per_tuple -= self.merge_qual_cost.per_tuple

        
        # projcost
        self.get_proj_cost()

        # skip_mark_restore表示是否跳过重复匹配
        self.skip_mark_restore = False
        if ((self.join_type == JoinType.SEMI or
            self.join_type == JoinType.ANTI or
            self.inner_unique == True) and 
            (len(self.restrict_list) == len(self.merge_cond))):   # 所有的约束条件都是可以归并的(即join filter应该为空), 并且满足一个outer tuple只会和一个inner tuple匹配
            self.skip_mark_restore = True

        # startsel, endsel
        self.outerstartsel, self.outerendsel, self.innerstartsel, self.innerendsel = erow.mergejoinscansel(self)



    def adjust_filter(self):
        super().adjust_filter()
        for mc in self.merge_cond:
            mc.replace_alias(self.simple_rel_alias_dict)
    def update_param(self, dict):
        super().update_param(dict)
        for mc in self.merge_cond:
            mc.updateParam(dict)
    def init_after_son_init(self):
        self.get_simple_rel_array()
        self.cal_semifactors()
        # selectivity
        self.get_selec()
        self.get_approx_selec(self.merge_cond)

    def recost_fun(self):
        mergejoin_info(self, self.children[0], self.children[1])
        

class NestLoopNode(JoinNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        if json_dict.get("Filter", None) != None:
            self.get_filter(json_dict["Filter"])
            self.extend_restrict_list(self.filter)
        if json_dict.get("Join Filter", None) != None:
            self.get_join_filter(json_dict["Join Filter"])
            self.extend_restrict_list(self.join_filter)
        
        # qual_cost
        self.cal_qual_cost()

        
        # projcost
        self.get_proj_cost()

        # get after processing
        self.join_qual_cost = None
    
    def init_after_son_init(self):
        self.get_simple_rel_array()
        self.cal_semifactors()
        # selectivity
        self.get_selec()

    def recost_fun(self):
        nestloop_info(self, self.children[0], self.children[1])

class SeqScanNode(ScanNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)

    def recost_fun(self):
        seqscan_info(self)

class IndexScanNode(ScanNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    
class IndexOnlyScanNode(ScanNode):
    def __init__(self, json_dict):
        super().__init__(json_dict)
class MaterialNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
    def recost_fun(self):
        materialize_info(self, self.children[0])
class SortNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
class UniqueNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        self.num_cols = None
    def recost_fun(self):
        unique_info(self, self.children[0])
class GatherMergeNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        self.num_workers = None
    def recost_fun(self):
        gathermerge_info(self, self.children[0])
class HashNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        self.num_workers = None
    def recost_fun(self):
        hash_info(self, self.children[0])
    

def nodeFactory(json_dict):
    for nt in NodeType:
        if nt.value == json_dict['Node Type']:
            node_type = nt
            break
    if node_type == NodeType.NESTED_LOOP:
        curr_node = NestLoopNode(json_dict)
    elif node_type == NodeType.HASH_JOIN:
        curr_node = HashJoinNode(json_dict)
    elif node_type == NodeType.MERGE_JOIN:
        curr_node = MergeJoinNode(json_dict)
    elif node_type == NodeType.SEQ_SCAN:
        curr_node = SeqScanNode(json_dict)
    elif node_type == NodeType.HASH:
        curr_node = HashNode(json_dict)
    elif node_type == NodeType.MATERIALIZE:
        curr_node = MaterialNode(json_dict)
    return curr_node