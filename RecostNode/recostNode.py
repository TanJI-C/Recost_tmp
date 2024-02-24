from enum import Enum
from typing import TypeVar
import util
from Recost_tmp.RecostNode.filterNode import parse_filter


# TODO: 初始化方法
class RestrictInfo:
    def __init__(self) -> None:
        # 约束条件可以分为连接条件和过滤条件, 这里不需要区分, 已经通过json_dict的join filter和filter区分了
        # 在mergejoin和hashjoin中,还会再将连接条件区分为是否mergeable和hashable, 同样不需要区分，已经通过merge cond, hash cond和join filter进行区分
        self.type = None   # 约束条件的类型包括: 
                                    # Var,Const,Param,Not,And,Or,OpExpr,ScalarArrayOpExpr,
                                    # RowCompareExpr,NullTest,BooleanTest,CurrentOfExpr,RelabelType,CoerceToDomain
        self.simple_rel_array = None # 当前约束语句涉及的relation, 可能为空


class Node(object):
    def __init__(self, json_dict):
        # get from explain
        self.node_type = json_dict["Node Type"]
        self.startup_cost = json_dict["Startup Cost"]
        self.total_cost = json_dict["Total cost"]
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
            self.filter = parse_filter(self.filter_src)

    def getSimplreRelArray(self, json_dict):
        # simple_rel_array
        for child in self.children:
            self.simple_rel_array.extend(child.simple_rel_array)
        if self.relation_name != None:
            self.simple_rel_array.append(self.relation_name)
    

    def extendRestrictList(self, restrict_list):
        if restrict_list != None:
            self.restrict_list.extend(restrict_list)

    def calQualCost(self):
        self.qp_qual_cost = util.cost_qual_eval(self.restrict_list, self)

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

class JoinNode(Node):
    def __init__(self, json_dict):
        super().__init__(json_dict)
        # get from explain
        self.join_type = json_dict["Join Type"]
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
        if self.join_type == "ANTI" or self.join_type == "SEMI" or self.inner_unique == True:
            joinquals = []
            if util.IS_OUTER_JOIN(self.join_type):
                joinquals.extend(self.join_filter)
            else:
                joinquals.extend(self.restrict_list)
            # TODO: 这里如何传入simple_rel_array参数比较合适?
            self.semifactors_outer_match_frac = util.clauselist_selectivity(self, joinquals, jointype="ANTI" if self.join_type == "ANTI" else "SEMI")
            self.semifactors_match_count = max(1.0, 
                util.clauselist_selectivity(self, joinquals, jointype="INNER") / self.semifactors_outer_match_frac * self.children[1].rows)

    def getJoinFilter(self, join_filter_src):
        self.join_filter_src = join_filter_src
        if self.join_filter_src != None:
            self.join_filter = parse_filter(self.join_filter_src)

    def extendJoinFilter(self, join_filter):
        if join_filter != None:
            self.join_filter.extend(join_filter)

    def getSelec(self):
        self.fkselec, self.jselec, self.pselec = util.get_join_selectivity(self, self.children[0], self.children[1])

    def getApproxSelec(self, restrict_list):
        self.approx_selec = util.clause_selectivity(self, restrict_list)

    def getProjCost(self):
        # TODO: proj_cost:
        self.proj_cost = []


class ScanNode(Node):
    def __init__(self, node_type):
        super().__init__(node_type)

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
        self.hash_cond = parse_filter(self.hash_cond_src)
        self.extendRestrictList(self.hash_cond)
        self.extendJoinFilter(self.hash_cond)

        # qual cost
        self.calQualCost()
        self.hash_qual_cost = util.cost_qual_eval(self.hash_cond, self)
        self.qp_qual_cost.startup -= self.hash_qual_cost.startup
        self.qp_qual_cost.per_tuple -= self.hash_qual_cost.per_tuple

        # selectivity
        self.getSelec()
        self.getApproxSelec(self.hash_cond)


        # projcost
        self.getProjCost()

        # get from recost_fun
        self.numbatches = None

        # get from static
        self.innerbucketsize, self.innermcvfreq = util.estimate_hash_bucket_stats()

    # 数据发生偏移的时候调用
    def updateWhenDataChange(self):
        self.innerbucketsize, self.innermcvfreq = util.estimate_hash_bucket_stats()

    def initAfterSonInit(self):
        self.calSemifactors()

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
        self.merge_cond = parse_filter(self.merge_cond_src)
        self.extendRestrictList(self.merge_cond)
        self.extendJoinFilter(self.merge_cond)

        # qual_cost
        self.calQualCost()
        self.merge_qual_cost = util.cost_qual_eval(self.merge_cond, self)
        self.qp_qual_cost.startup -= self.merge_qual_cost.startup
        self.qp_qual_cost.per_tuple -= self.merge_qual_cost.per_tuple

        # selectivity
        self.getSelec()
        self.getApproxSelec(self.merge_cond)
        
        # projcost
        self.getProjCost()

        # skip_mark_restore表示是否跳过重复匹配
        self.skip_mark_restore = False
        if ((self.jointype == "JOIN_SEMI" or
            self.jointype == "JOIN_ANTI" or
            self.inner_unique == True) and 
            (len(self.restrict_list) == len(self.merge_cond))):   # 所有的约束条件都是可以归并的(即join filter应该为空), 并且满足一个outer tuple只会和一个inner tuple匹配
            self.skip_mark_restore = True

        # startsel, endsel
        self.outerstartsel, self.outerendsel, self.innerstartsel, self.innerendsel = util.mergejoinscansel(self)

    def initAfterSonInit(self, json_dict):
        self.getSimplreRelArray()
        self.calSemifactors()


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

        # selectivity
        self.getSelec()
        
        # projcost
        self.getProjCost()

        # get after processing
        self.join_qual_cost = None
    
    def initAfterSonInit(self, json_dict):
        self.getSimplreRelArray()
        self.calSemifactors()

class IndexScanNode(ScanNode):
    def __init__(self, node_type):
        super().__init__(node_type)

class IndexOnlyScanNode(ScanNode):
    def __init__(self, node_type):
        super().__init__(node_type)

class MaterialNode(Node):
    def __init__(self, node_type):
        super().__init__(node_type)

class SortNode(Node):
    def __init__(self, node_type):
        super().__init__(node_type)

class UniqueNode(Node):
    def __init__(self, node_type):
        super().__init__(node_type)
        self.num_cols = None

class GatherMergeNode(Node):
    def __init__(self, node_type):
        super().__init__(node_type)
        self.num_workers = None

DerivedPlanNode = TypeVar('DerivedPlanNode', bound=Node)
DerivedJoinNode = TypeVar('DerivedJoinNode', bound=JoinNode)




# 统计信息: 参考PG_STATIC的实现
class StatisticInfo:
    def __init__(self) -> None:
        self.relation_name = None
        self.pages = None
        self.tuples = None
        self.column_sta = {}
        self.multi_column_sta = {}

class StatisticKind(Enum):
    STATISTIC_KIND_MCV = 1
    STATISTIC_KIND_HISTOGRAM = 2
    STATISTIC_KIND_CORRELATION = 3
    STATISTIC_KIND_MCELEM = 4
    STATISTIC_KIND_DECHIST = 5
    STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM = 6
    STATISTIC_KIND_BOUNDS_HISTOGRAM = 7

class ColumnStatisticInfo:
    UNIQUE_DISTINCT = -1.0
    def __init__(self) -> None:
        # 后面初始化得到的
        self.minval = None
        self.maxval = None
        # 统计表里带的
        self.datatype = None        # 数据类型
        self.nullfrac = None
        self.width = None
        self.distinct = None
        self.stakind = []
        self.staop = []
        self.stanumbers = []
        self.stavalues = []

class MultiColumnStatisticInfo:
    def __init__(self) -> None:
        self.keys = None            # List[str] 统计哪些列, 后面全部对应的使用数字,
        self.kind = None            # List[char] d表示distinct,f表示dependencies
        self.ndistinct = None       # map(str, int)  表示多列不同的个数
        self.dependencies = None    # map(str, float)   表示多列的依赖度