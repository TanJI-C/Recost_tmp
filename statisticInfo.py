
from enum import Enum
from Recost_tmp.PlanNode.filterNode import PredicateNode


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




def init_statistic():
    # 定义为全局变量
    global StatisticInfoOfRel
    StatisticInfoOfRel = {}

def get_statistic_of_rel(relation: PredicateNode):
    # 检查是不是
    if len(relation.children) == 0 and len(relation.table_array) == 1:
        # TODO: 约定text第一个维度为表格全称, 第二个维度为属性全称
        return StatisticInfoOfRel.get(relation.text[0], StatisticInfo())
    # 返回一个空的统计信息
    return StatisticInfo()

def get_statistic_of_col(rel_sta: StatisticInfo, column: PredicateNode):
    if len(column.children) == 0 and len(column.table_array) == 1:
        # TODO: 约定text第一个维度为表格全称, 第二个维度为属性全称
        return rel_sta.column_sta.get(column.text[1], None)
    # 返回None
    return None