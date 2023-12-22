import logging
import math
from typing import Type, TypeVar

cpu_operator_cost = 0.0025
cpu_tuple_cost = 0.01
seq_page_cost = 1.0

HJTUPLE_OVERHEAD = 16 # typealign(8, sizeof (HashJoinTupleData))
SizeofMinimalTupleHeader = 32 
SizeofHeapTupleHeader = 32      
BLCKSZ = 8192
work_mem = 1024

SKEW_WORK_MEM_PRECENT = 2
SKEW_BUCKET_OVERHEAD = 16

HashJoinTupleSize = 8

MaxAllocSize = 0x3fffffff


INT_MAX = 0x7fffffff

NTUP_PER_BUCKET = 1


class Cost:
    def __init__(self):
        self.startup = None
        self.per_tuple = None

class Node(object):
    def __init__(self, node_type, table_name=None, cost=None):
        self.node_type = node_type
        self.total_cost = 0
        self.actual_time_ms = 0
        self.info = {}
        self.children = []

        # set for leaves(叶子scan节点需要用到)
        self.table_name = table_name
        self.table_alias = None

        # Internal cached fields
        self._card = None   # Used in MinCardCost #? 不是很理解
        self._leaf_scan_op_copies = {}

        # change: 自定义
        self.rows = None
        self.startup_cost = None
        self.width = None
        self.unique = False
        
        # change: 
    
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
    def __init__(self, node_type, jointype):
        super().__init__(node_type)
        self.jointype = jointype
        self.restrictlist = []
        self.proj_cost = None
        self.semifactors = None

class ScanNode(Node):
    def __init__(self, node_type):
        super().__init__(node_type)

class HashJoinNode(JoinNode):
    def __init__(self, node_type, jointype, hahsclauses):
        super().__init__(node_type, jointype)
        self.hashclauses = hahsclauses
        self.fkselec = None
        self.jselec = None
        self.pselec = None
    # TODO: 给定json之后, 进行hashjoin node需要的信息初始化, 最后返回hashjoin node
    def node_init(json_dict, node: Node): 
        pass

class MergeJoinNode(JoinNode):
    def __init__(self, node_type, jointype, mergeclauses):
        super().__init__(node_type, jointype)
        self.mergeclauses = mergeclauses
        self.skip_mark_restore = False
        self.materialize_inner = False
        self.outerstartsel = None
        self.outerendsel = None
        self.innerstartsel = None
        self.innerendsel = None
        self.innersortkeys = None
    # TODO: 给定json之后, 进行mergejoin node需要的信息初始化, 最后返回mergejoin node
    def node_init(json_dict, node: Node):
        pass

class NestLoopNode(JoinNode):
    def __init__(self, node_type, jointype):
        super().__init__(node_type, jointype)
    # TODO: 给定json之后, 进行nestloop node需要的信息初始化, 最后返回nestloop node
    def node_init(json_dict, node: Node):
        pass

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


DerivedPlanNode = TypeVar('DerivedPlanNode', bound=Node)


def typealign(alignment, length):
    return (length + (alignment - 1)) & ~(alignment - 1)

def clamp_row_est(rows):
    if rows <= 1.0:
        rows = 1.0
    else:
        rows = round(rows)
    return rows

# 给定表格的元组数量和元组宽度, 返回表格的字节大小
def relation_byte_size(ntuples, tupwidth):
    return ntuples * (typealign(8, tupwidth) + typealign(8, SizeofHeapTupleHeader))
# 给定tuples数量和tuple宽度, 返回页数
def page_size(ntuples, tupwidth):
    return math.ceil(relation_byte_size(ntuples, tupwidth)) / BLCKSZ



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

# 解析json格式的plan为node
def ParsePostgresPlanJson(json_dict):
    """Takes JSON dict, parses into a Node."""
    curr = json_dict['Plan']

    def _parse_pg(json_dict, select_exprs=None, indent=0):
        op = json_dict['Node Type']
        cost = json_dict['Total Cost']
        if op == 'Aggregate':
            op = json_dict['Partial Mode'] + op
            if select_exprs is None:
                # Record the SELECT <select_exprs> at the topmost Aggregate.
                # E.g., ['min(mi.info)', 'min(miidx.info)', 'min(t.title)'].
                select_exprs = json_dict['Output']
        
        # Record relevant info.
        curr_node = Node(op)
        curr_node.cost = cost
        # change: 记录startup_cost, width, rows
        curr_node.startup_cost = json_dict.get('Startup Cost')
        curr_node.width = json_dict.get('Plan Width')
        curr_node.rows = json_dict.get("Plan Rows")
        curr_node.unique = json_dict.get("Inner Unqiue", default=False)

        # Only available if 'analyze' is set (actual execution).
        curr_node.actual_time_ms = json_dict.get('Actual Total Time')
        # Special case.
        if 'Relation Name' in json_dict:
            curr_node.table_name = json_dict['Relation Name']
            curr_node.table_alias = json_dict['Alias']

        # Unary predicate on a table.
        if 'Filter' in json_dict:
            assert 'Scan' in op, json_dict
            assert 'Relation Name' in json_dict, json_dict
            curr_node.info['filter'] = json_dict['Filter']

        if 'Scan' in op and select_exprs:
            # Record select exprs that belong to this leaf.
            # Assume: SELECT <exprs> are all expressed in terms of aliases.
            if 'Alias' in json_dict.keys():
                filtered = _FilterExprsByAlias(select_exprs, json_dict['Alias'])
                if filtered:
                    curr_node.info['select_exprs'] = filtered

        # change: 调用对应的函数,进行节点初始化
        if 'Hash Join' in op:
            curr_node = HashJoinNode.node_init(json_dict, curr_node)
        elif 'Merge Join' in op:
            curr_node = MergeJoinNode.node_init(json_dict, curr_node)
        elif 'Nested Loop' in op:
            curr_node = NestLoopNode.node_init(json_dict, curr_node)


        # Recurse.
        if 'Plans' in json_dict:
            for n in json_dict['Plans']:
                curr_node.children.append(
                    _parse_pg(n, select_exprs=select_exprs, indent=indent + 2))
        if op == 'Bitmap Heap Scan':
            for c in curr_node.children:
                if c.node_type == 'Bitmap Index Scan':
                    # 'Bitmap Index Scan' doesn't have the field 'Relation Name'.
                    c.table_name = curr_node.table_name
                    c.table_alias = curr_node.table_alias

        return curr_node

    return _parse_pg(curr)
