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
        # 
        self.est_rows = None
        self.est_startup_cost = None
        self.est_total_cost = None
    @abstractmethod    
    def get_filter(self, filter_str):
        pass
    @abstractmethod
    def get_simple_rel_array(self, json_dict):
        pass
    @abstractmethod
    def adjust_filter(self):
        pass
    @abstractmethod    
    def extend_restrict_list(self, restrict_list):
        pass
    @abstractmethod    
    def cal_qual_cost(self):
        pass
    @abstractmethod    
    def with_alias(self, alias):
        pass
    @abstractmethod
    def get_table_id(self, with_alias=True, alias_only=False):
        pass
    @abstractmethod
    def init_after_son_init(self):
        pass
    @abstractmethod
    def update_param(self, dict):
        pass
    @abstractmethod
    def recost_fun(self):
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
    def cal_semifactors(self):
        pass
    @abstractmethod
    def get_join_filter(self, join_filter_src):
        pass
    @abstractmethod
    def extend_join_filter(self, join_filter):
        pass
    @abstractmethod
    def get_selec(self):
        pass
    @abstractmethod
    def get_approx_selec(self, restrict_list):
        pass
    @abstractmethod
    def get_proj_cost(self):
        pass

class ScanNodeInterface(PlanNodeInterface):
    def __init__(self) -> None:
        super().__init__()


