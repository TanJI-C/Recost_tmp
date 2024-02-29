from enum import Enum
from typing import TypeVar
from abc import ABC, abstractmethod

class NodeType(Enum):
    NESTED_LOOP = 'Nested Loop'
    HASH_JOIN = 'Hash Join'
    MERGE_JOIN = 'Merge Join'

    SEQ_SCAN = "Seq Scan"
    FUNCTION_SCAN = 'Function Scan'
    CTE_SCAN = 'CTE Scan'
    WORK_TABLE_SCAN = 'WorkTable Scan'
    INDEX_SCAN = 'Index Scan'
    INDEX_ONLY_SCAN = 'Index Only Scan'
    BITMAP_HEAP_SCAN = 'Bitmap Heap Scan'
    MATERIALIZE = 'Materialize'
    SORT = 'Sort'
    UNIQUE = 'Unique'
    HASH = 'Hash'
    
    

class PlanNodeInterface(ABC):
    def __init__(self) -> None:
        super().__init__()
        # get from explain
        self.node_type = None
        self.startup_cost = None
        self.total_cost = None
        self.rows = None
        self.width = None
        self.actual_startup_time = None
        self.actual_total_time = None
        self.actual_rows = None
        self.output = None      #??? 似乎没有
        self.relation_name = None
        # init in initialization of derived class
        self.filter_src = None          # 过滤条件(字符串格式)
        self.filter = None             # 过滤条件(restrictInfo格式)
        self.simple_rel_array = None     # 所有涉及的简单表格
        self.restrict_list = None      # 所有约束条件(restrictInfo格式)
        self.qp_qual_cost = None        # 全部约束条件的cost
        # init out-of-class
        self.children = None
        self.recost_fun = None
    @abstractmethod    
    def getFilter(self, filter_str):
        pass
    @abstractmethod
    def getSimpleRelArray(self, json_dict):
        pass
    @abstractmethod    
    def extendRestrictList(self, restrict_list):
        pass
    @abstractmethod    
    def calQualCost(self):
        pass
    @abstractmethod    
    def with_alias(self, alias):
        pass
    @abstractmethod
    def get_table_id(self, with_alias=True, alias_only=False):
        pass
    @abstractmethod
    def initAfterSonInit(self):
        pass

class JoinType(Enum):
    INNER = 'Inner'
    FULL = 'Full'
    LEFT = 'Left'
    RIGHT = 'Right'
    SEMI = 'Semi'
    ANTI = 'Anti'

class JoinNodeInterface(PlanNodeInterface):
    def __init__(self) -> None:
        super().__init__()
        self.join_type = None
        self.inner_unique = None
        self.rows_removed_by_filter = None
        # init after son init
        self.semifactors_outer_match_frac = None
        self.semifactors_match_count = None
        self.join_filter_src = None
        self.join_filter = None
        self.fkselec = None
        self.jselec = None
        self.pselec = None       
        self.approx_selec = None #用于大致估计行数
        self.proj_cost = None
    @abstractmethod
    def calSemifactors(self):
        pass
    @abstractmethod
    def getJoinFilter(self, join_filter_src):
        pass
    @abstractmethod
    def extendJoinFilter(self, join_filter):
        pass
    @abstractmethod
    def getSelec(self):
        pass
    @abstractmethod
    def getApproxSelec(self, restrict_list):
        pass
    @abstractmethod
    def getProjCost(self):
        pass

class ScanNodeInterface(PlanNodeInterface):
    def __init__(self) -> None:
        super().__init__()



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