from enum import Enum
import math
import re
from typing import TypeVar
from Recost_tmp.util import CustomList

estimated_regex = re.compile(
    '\(cost=(?P<est_startup_cost>\d+.\d+)..(?P<est_cost>\d+.\d+) rows=(?P<est_card>\d+) width=(?P<est_width>\d+)\)')
actual_regex = re.compile('\(actual time=(?P<act_startup_cost>\d+.\d+)..(?P<act_time>\d+.\d+) rows=(?P<act_card>\d+)')
op_name_regex = re.compile('->  ([^"(]+)')
workers_planned_regex = re.compile('Workers Planned: (\d+)')
# filter_columns_regex = re.compile('("\S+"."\S+")')
filter_columns_regex = re.compile('([^\(\)\*\+\-\'\= ]+)')
literal_regex = re.compile('(\'[^\']+\'::[^\'\)]+)')


class Cost:
    def __init__(self):
        self.startup = 0
        self.per_tuple = 0

class Operator(Enum):
    NEQ = '!='
    EQ = '='
    LEQ = '<='  # 忽略<和<=的差异
    GEQ = '>='
    LIKE = 'LIKE'
    NOT_LIKE = 'NOT LIKE'
    IS_NOT_NULL = 'IS NOT NULL'
    IS_NULL = 'IS NULL'
    IN = 'IN'
    BETWEEN = 'BETWEEN'
    IS_TRUE = 'IS TRUE'
    IS_NOT_TRUE = 'IS NOT TRUE'
    IS_FALSE = 'IS FALSE'
    IS_NOT_FALSE = 'IS NOT FALSE'
    IS_UNKNOWN = 'IS UNKNOWN'
    IS_NOT_UNKNOWN = 'IS NOT UNKNOWN'
    IS_DISTINCT_FROM = '=' # 实现上与EQ相同, 最后计算时取反即可

    PLUS = '+'
    MINUS = '-'
    MULTI = '*'
    DIVI = '/'
    MOD = '%'

    COERCE = '::'

    ILLEGAL_OPERATOR = '-1'

    P_FUN = '_p'

    SUBSTRING_FUN = '"substring'

    def __str__(self):
        return self.value

class LogicalOperator(Enum):
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'

    def __str__(self):
        return self.value

class Aggregator(Enum):
    AVG = 'AVG'
    SUM = 'SUM'
    COUNT = 'COUNT'

    def __str__(self):
        return self.value

class ExtendedAggregator(Enum):
    MIN = 'MIN'
    MAX = 'MAX'

    def __str__(self):
        return self.value

class Restrict(Enum):
    Var = 'Var'
    Const = 'Const'
    Param = 'Param'
    NotClause = 'NotClause'
    AndClause = 'AndClause'
    OrClause = 'OrClause'
    OpExpr = 'OpExpr'
    DistinctExpr = 'DistinctExpr'
    ScalarArrayOpExpr = 'ScalarArrayOpExpr'
    RowCompareExpr = 'RowCompareExpr'
    NullTest = 'NullTest'
    BooleanTest = 'BooleanTest'
    CurrentOfExpr = 'CurrentOfExpr'
    RelabelType = 'RelabelType'
    CoerceToDomain = 'CoerceToDomain'
    def __str__(self):
        return self.value

class PredicateNode:
    def __init__(self, text, children, children_offset):
        self.text = text
        self.children = children
        self.children_offset = children_offset
        self.operator = None
        # change: is_p exist
        self.p_arg_array = CustomList()
        # change: to cost and row estimate
        self.type = None
        self.cost = Cost()
        self.selec = None
        self.is_join = False
        self.table_array = CustomList()
        self.coerce_num = 0
        self.text_is_null = False

    def to_dict(self):
        return dict(
            type = self.type.value,
            operator= self.operator.value if self.operator != None else None,
            text = self.text,

            cost = [self.cost.startup, self.cost.per_tuple],
            selec = self.selec,
            is_join = self.is_join,
            p_arg_array = self.p_arg_array,
            table_array = self.table_array,
            coerce_num = self.coerce_num,
            children=[c.to_dict() for c in self.children]
        )

    def lookup_columns(self, plan, **kwargs):
        if self.column is not None:
            self.column = plan.lookup_column_id(self.column, **kwargs)
        for c in self.children:
            c.lookup_columns(plan, **kwargs)

    def GetConst(self):
        text = self.text
        if '::text[]' in text or '::bpchar[]' in text or '::date[]' in text or '::time without time zone[]' in text:
            self.text = [c.strip('"\'') for c in text.split('::')[0].strip('"\'').strip('{}').split(",")]
            self.coerce_num += len(self.text)
        elif '::text' in text or '::bpchar' in text or '::date' in text or '::time without time zone' in text:
            self.text = text.split("::")[0].strip("'")
            self.coerce_num += 1
        elif '::double precision[]' in text or '::numeric[]' in text or '::integer[]' in text:
            self.text = [float(c) for c in text.split('::')[0].strip('"\'').strip('{}').split(",")]
            self.coerce_num += len(self.text)
        elif '::double precision' in text or '::numeric' in text or '::integer' in text:
            self.text = float(text.split("'::")[0].strip("'"))
            self.coerce_num += 1
        elif re.match(r"\D\w*\.\D\w*", text.strip()):
            # column comparison. ignored.
            # self.text = None
            # change:
            self.text = tuple(text.split('.'))
            return Restrict.Var
        elif re.match(r"\D\w*", text.strip()): # 没有双引号包括时，认为是列
            self.text = text.strip()
            return Restrict.Var
        elif re.match(r"[-+]?(\d+(\.\d*)?|\.\d+)", text.strip()): # 匹配实数(不存在科学计数法)
            self.text = float(text.strip())
        else:
            try:
                self.text = float(text.strip())
                self.coerce_num += 1
            except ValueError:
                #print(
                #    f"Could not parse self.text {text} (maybe a join condition? if so, this can be ignored)")
                self.text = None
        return Restrict.Const

    def replace_alias(self, dict):
        # TODO: is_join的判断为涉及的列属性为两个以上
        for children in self.children:
            children.replace_alias(dict)
        for idx, _ in enumerate(self.table_array):
            self.table_array[idx] = dict.get(self.table_array[idx], self.table_array[idx])
        if (self.type == Restrict.OpExpr or self.type == Restrict.DistinctExpr) and len(self.table_array) == 2:
            self.is_join = True

    # 主要处理放置在text中的类型转化符
    def adjust_tree(self):
        # 无用的括号
        if self.text.strip() == '' and len(self.children) == 1:
            self.children[0].adjust_tree()
            self.text = self.children[0].text
            self.operator = self.children[0].operator
            self.p_arg_array.extend(self.children[0].p_arg_array)
            self.type = self.children[0].type
            self.is_join = self.children[0].is_join
            self.coerce_num = self.children[0].coerce_num
            self.table_array = self.table_array
            
            self.children = self.children[0].children
            return

        keywords = [w.strip() for w in self.text.split(' ') if len(w.strip()) > 0]
        if all([k == 'AND' for k in keywords]):
            self.operator = LogicalOperator.AND
            self.type = Restrict.AndClause
        elif all([k == 'OR' for k in keywords]):
            self.operator = LogicalOperator.OR
            self.type = Restrict.OrClause
        elif len(keywords) == 1 and keywords[0] == 'NOT':
            self.operator = LogicalOperator.NOT
            self.type = Restrict.NotClause
        else:
        # 逻辑运算符不需要调整
        # 保证text至多出现下面的运算符一次，text只可能包含类型转换运算符和_p函数其中一种
            repr_op = [
                ('= ANY', Operator.IN, Restrict.ScalarArrayOpExpr),
                ('=', Operator.EQ, Restrict.OpExpr),
                ('>=', Operator.GEQ, Restrict.OpExpr),
                ('>', Operator.GEQ, Restrict.OpExpr),
                ('<=', Operator.LEQ, Restrict.OpExpr),
                ('<', Operator.LEQ, Restrict.OpExpr),
                ('<>', Operator.NEQ, Restrict.OpExpr),
                ('~~', Operator.LIKE, Restrict.OpExpr),
                ('!~~', Operator.NOT_LIKE, Restrict.OpExpr),
                ('IS NOT NULL', Operator.IS_NOT_NULL, Restrict.NullTest),
                ('IS NULL', Operator.IS_NULL, Restrict.NullTest),
                ('IS TRUE', Operator.IS_TRUE, Restrict.BooleanTest),
                ('IS NOT TRUE', Operator.IS_NOT_TRUE, Restrict.BooleanTest),
                ('IS FALSE', Operator.IS_FALSE, Restrict.BooleanTest),
                ('IS NOT FALSE', Operator.IS_NOT_FALSE, Restrict.BooleanTest),
                ('IS UNKNOWN', Operator.IS_UNKNOWN, Restrict.BooleanTest),
                ('IS NOT UNKNOWN', Operator.IS_NOT_UNKNOWN, Restrict.BooleanTest),
                ('IS DISTINCT FROM', Operator.IS_DISTINCT_FROM, Restrict.DistinctExpr),
                ('+', Operator.PLUS, Restrict.Var),
                ('-', Operator.MINUS, Restrict.Var),
                ('*', Operator.MULTI, Restrict.Var),
                ('/', Operator.DIVI, Restrict.Var),
                ('%', Operator.MOD, Restrict.Var),
            ]
            node_op = None
            type = None
            for op_rep, op, rtype in repr_op:
                split_str = f' {op_rep} '
                self.text = self.text + ' '
                if split_str not in self.text: # match
                    continue
                left_text = self.text.split(split_str)[0]
                right_text = self.text.split(split_str)[1]
                op_idx = len(left_text)
                node_op = op
                type = rtype
                    # left_text不为空, 则进行一次封装
                if left_text.strip() != '':
                    left = PredicateNode(left_text, [], [])
                    if len(self.children) >= 1:
                        # 如果有左孩子了，将其作为自己的孩子
                        if self.children_offset[0] <= op_idx:
                            left.children.append(self.children[0])
                            self.children[0] = left
                        else: # 仅有右孩子则放置在左边
                            # (仅可能是类型转换运算符转换常量，或者仅变量，或者仅常量)
                            # if left.GetConst() == Restrict.Var:
                            #     left.type = Restrict.Var
                            # else:
                            #     left.type = Restrict.Const
                            self.children = [left, self.children[0]]
                    else:
                        # if left.GetConst() == Restrict.Var:
                        #     left.type = Restrict.Var
                        # else:
                        #     left.type = Restrict.Const
                        self.children.append(left)

                if right_text.strip() != '':
                    right = PredicateNode(right_text, [], [])
                    right.text = right_text
                    # 经过left处理之后左边一定有孩子, 如果是一个表明原来没有右孩子, 如果是两个则表明有
                    if len(self.children) == 1:
                        # if right.GetConst() == Restrict.Var:
                        #     right.type = Restrict.Var
                        #     right.is_table_exist = True
                        # else:
                        #     right.type = Restrict.Const
                        self.children.append(right)
                    else:
                        right.children.append(self.children[-1])
                        self.children[-1] = right
                
                self.text = split_str
                break
        
            # 可能为类型转化运算符(单目), _P函数, 变量, 常量; 对于前2个则进行封装
            if type == None:
                # 变量+常量（递归基）
                if len(self.children) == 0:
                    if self.GetConst() == Restrict.Var:
                        self.type = Restrict.Var
                        # 只有列名的, 表格的名字会在replace_alias函数中补完
                        self.table_array.append('omit' if len(self.text) == 1 else self.text[0])
                    else:
                        self.type = Restrict.Const
                    return

                if Operator.COERCE.value in self.text:
                    node_op = Operator.COERCE
                    type = Restrict.Var
                    self.coerce_num += 1
                elif Operator.P_FUN.value in self.text:
                    node_op = Operator.P_FUN
                    type = Restrict.Var
                    # TODO: _p key_word
                    self.p_arg_array.append('tmp')
                elif Operator.SUBSTRING_FUN.value in self.text:
                    node_op = Operator.SUBSTRING_FUN
                    type = Restrict.Var
                    # 将列提取出来
                    self.children[0] = self.children[0].children[0]

            self.operator = node_op
            self.type = type

        for idx, _ in enumerate(self.children):
            self.children[idx].adjust_tree()
            self.coerce_num += self.children[idx].coerce_num
            self.p_arg_array.extend(self.children[idx].p_arg_array)
            self.table_array.extend(self.children[idx].table_array)
            if len(self.children[idx].children) == 0 and (self.operator == Operator.COERCE or self.operator == Operator.P_FUN):
                self.text = self.children[idx].text
                self.operator = self.children[idx].operator
                self.type = self.children[idx].type
                self.children = []
                break
            
    # TODO: 检查树的正确性
    def check_tree(self):
        return True

    def updateParam(self, dict):
        for children in self.children:
            children.updateParm(dict)
        # 自己当前层有_p, 且没有儿子了(_p函数里要求是单一的常数)
        if len(self.children) == 0 and self.type == Restrict.Cosnt and len(self.p_arg_array) == 1:
            self.text = dict.get(self.p_arg_array[0], self.text)


    # def parse_lines_recursively(self, parse_baseline=False):
    #     self.parse_lines(parse_baseline=parse_baseline)
    #     for c in self.children:
    #        c.parse_lines_recursively(parse_baseline=parse_baseline)
    #        # 如果儿子有_p函数, 也将当前的flag设置未True
    #        if c.is_p_exist == True:
    #            self.is_p_exist = True
    #    # remove any children that have no literal
    #    if parse_baseline:
    #        self.children = [c for c in self.children if
    #                         c.operator in {LogicalOperator.AND, LogicalOperator.OR, LogicalOperator.NOT,
    #                                        Operator.IS_NOT_NULL, Operator.IS_NULL, Operator.IS_TRUE, Operator.IS_NOT_TRUE, 
    #                                        Operator.IS_FALSE, Operator.IS_NOT_FALSE, Operator.IS_UNKNOWN, Operator.IS_NOT_UNKNOWN}
    #                         or c.literal is not None]


    # def parse_lines(self, parse_baseline=False):
    #     keywords = [w.strip() for w in self.text.split(' ') if len(w.strip()) > 0]
    #     for idx, _ in enumerate(self.children):
    #         self.children[idx] = PredicateNode.simplify_vars(self.children[idx])
    #         self.coerce_num += self.children[idx].coerce_num
    #         if self.children[idx].is_p_exist == True:
    #             self.is_p_exist = True
    #     if all([k == 'AND' for k in keywords]):
    #         self.operator = LogicalOperator.AND
    #         self.type = Restrict.AndClause
    #     elif all([k == 'OR' for k in keywords]):
    #         self.operator = LogicalOperator.OR
    #         self.type = Restrict.OrClause
    #     elif len(keywords) == 1 and keywords[0] == 'NOT':
    #         self.operator = LogicalOperator.NOT
    #         self.type = Restrict.NotClause
    #     else:
    #         repr_op = [
    #             ('= ANY', Operator.IN, Restrict.ScalarArrayOpExpr),
    #             ('=', Operator.EQ, Restrict.OpExpr),
    #             ('>=', Operator.GEQ, Restrict.OpExpr),
    #             ('>', Operator.GEQ, Restrict.OpExpr),
    #             ('<=', Operator.LEQ, Restrict.OpExpr),
    #             ('<', Operator.LEQ, Restrict.OpExpr),
    #             ('<>', Operator.NEQ, Restrict.OpExpr),
    #             ('~~', Operator.LIKE, Restrict.OpExpr),
    #             ('!~~', Operator.NOT_LIKE, Restrict.OpExpr),
    #             ('IS NOT NULL', Operator.IS_NOT_NULL, Restrict.NullTest),
    #             ('IS NULL', Operator.IS_NULL, Restrict.NullTest),
    #             ('IS TRUE', Operator.IS_TRUE, Restrict.BooleanTest),
    #             ('IS NOT TRUE', Operator.IS_NOT_TRUE, Restrict.BooleanTest),
    #             ('IS FALSE', Operator.IS_FALSE, Restrict.BooleanTest),
    #             ('IS NOT FALSE', Operator.IS_NOT_FALSE, Restrict.BooleanTest),
    #             ('IS UNKNOWN', Operator.IS_UNKNOWN, Restrict.BooleanTest),
    #             ('IS NOT UNKNOWN', Operator.IS_NOT_UNKNOWN, Restrict.BooleanTest),
    #             ('IS DISTINCT FROM', Operator.IS_DISTINCT_FROM, Restrict.DistinctExpr),
    #         ]
    #         node_op = None
    #         literal = None
    #         column = None
    #         filter_feature = 0
    #         type = None
    #         for op_rep, op, restrict in repr_op:
    #             split_str = f' {op_rep} '
    #             self.text = self.text + ' '

    #             if split_str in self.text: # match
    #                 assert node_op is None and type is None
    #                 node_op = op
    #                 type = restrict
    #                 literal = self.text.split(split_str)[1]
    #                 column = self.text.split(split_str)[0]
    #                 # change: 得到split_str的address, 用于判断children在左还是右
    #                 op_idx = len(column)

    #                 # dirty hack to cope with substring calls in
    #                 is_substring = self.text.startswith('"substring')
    #                 if is_substring:
    #                     self.children[0] = self.children[0].children[0]

    #                 # current restriction: filters on aggregations (i.e., having clauses) are not encoded using
    #                 # individual columns
    #                 # change: 进行了left children的判断
    #                 agg_ops = {'sum', 'min', 'max', 'avg', 'count'}
    #                 is_having = column.lower() in agg_ops or (len(self.children) >= 1 and self.children_offset[0] <= op_idx
    #                                                           and self.children[0].text in agg_ops)
    #                 if is_having:
    #                     # ? 不处理聚簇函数?
    #                     column = None
    #                     self.children = []
    #                 else:

    #                     # sometimes column is in parantheses
    #                     if len(self.children) >= 1 and self.children_offset[-1] > op_idx:
    #                         # 将literal并入节点之中，然后进行简化
    #                         right = PredicateNode()
    #                         right.text = literal
    #                         right.children.append(self.children[-1])
    #                         right.children_offset.append(self.children_offset[-1] - op_idx - len(split_str))
    #                         right = PredicateNode.simplify_vars(right)
    #                         self.children[-1] = right
    #                         if len(self.children[-1].children) == 0:
    #                             literal = self.children[-1].text
    #                             self.coerce_num += self.children[-1].coerce_num
    #                             self.children.pop()
    #                             self.children_offset.pop()
    #                         else:
    #                             literal = None
    #                     # 左var可以化简
    #                     if len(self.children) >= 1 and self.children_offset[0] <= op_idx:
    #                         left = PredicateNode()
    #                         left.text = column
    #                         left.children.append(self.children[0])
    #                         left.children_offset(self.children_offset[0])
    #                         left = PredicateNode.simplify_vars(left)
    #                         self.children[0] = left
    #                         if len(self.children[0].children) == 0:
    #                             column = self.children[0].text  # children[0]  = ANY    children[1]
    #                             self.coerce_num += self.children[0].coerce_num
    #                             self.children.pop()
    #                             self.children_offset.pop()
    #                         else:
    #                             literal = None
    #                     # if node_op == Operator.IN:
    #                     # 
    #                     #     # 右var可以化简
    #                     #     pass
    #                     # elif len(self.children) >= 1:
    #                     #     # 右var可以化简
    #                     #    pass
    #                     # elif len(self.children) == 0:
    #                     #     pass
    #                     # else:
    #                     #     raise NotImplementedError

    #                     # change: 假设var始终在左边
    #                     # # column and literal are sometimes swapped
    #                     # type_suffixes = ['::bpchar']
    #                     # if any([column.endswith(ts) for ts in type_suffixes]):
    #                     #     tmp = literal
    #                     #     literal = column
    #                     #     column = tmp.strip()

    #                     # additional features for special operators
    #                     # number of values for in operator
    #                     if node_op == Operator.IN:
    #                         filter_feature = literal.count(',')
    #                     # number of wildcards for LIKE
    #                     elif node_op == Operator.LIKE or node_op == Operator.NOT_LIKE:
    #                         filter_feature = literal.count('%')

    #                     break
    #         # change: 对单个Var也进行解析
    #         if node_op == None:
    #             tmp = PredicateNode(self)
    #             self.coerce_num += tmp.coerce_num
    #             self.is_p_exist = True

    #             self.type = Restrict.Var

    #         # 要求解析数据类型
    #         parse_baseline = True
    #         if parse_baseline:
    #             if node_op in {Operator.IS_NULL, Operator.IS_NOT_NULL, Operator.IS_TRUE, Operator.IS_NOT_TRUE, 
    #                             Operator.IS_FALSE, Operator.IS_NOT_FALSE, Operator.IS_UNKNOWN, Operator.IS_NOT_UNKNOWN}:
    #                 literal = None
    #             elif node_op == Operator.IN:

    #                 datatype = literal.split('::')[1].strip()
    #                 literal = literal.split('::')[0].strip("'").strip("{}")
    #                 # 注意区分原本的类型
    #                 if 'text' in datatype or 'bpchar' in datatype:
    #                     literal = [c.strip('"') for c in literal.split(',')]
    #                 elif 'date' in literal: # 'yyyy-mm-dd'
    #                     literal = [c.strip('"') for c in literal.split(',')]
    #                 elif 'time without time zone' in datatype: #'hh:mm:ss'
    #                     literal = [c.strip('"') for c in literal.split(',')]
    #                 elif 'integer' in datatype or 'numeric' or 'double precision':
    #                     literal = [float(c) for c in literal.split(',')]
    #             else:
    #                 if '::text' in literal:
    #                     literal = literal.split("'::text")[0].strip("'")
    #                 elif '::bpchar' in literal:
    #                     literal = literal.split("'::bpchar")[0].strip("'")
    #                 elif '::date' in literal:
    #                     literal = literal.split("'::date")[0].strip("'")
    #                 elif '::time without time zone' in literal:
    #                     literal = literal.split("'::time")[0].strip("'")
    #                 elif '::double precision' in literal:
    #                     literal = float(literal.split("'::double precision")[0].strip("'"))
    #                 elif '::numeric' in literal:
    #                     literal = float(literal.split("'::numeric")[0].strip("'"))
    #                 elif '::integer' in literal:
    #                     literal = float(literal.split("'::integer")[0].strip("'"))
    #                 elif re.match(r"\D\w*\.\D\w*", literal.replace('"', '').replace('\'', '').strip()):
    #                     # column comparison. ignored.
    #                     # literal = None
    #                     # change:
    #                     literal = tuple(literal.split('.'))
    #                     self.is_join = True
    #                 elif re.match(r"\D\w*", literal.strip()): # 没有双引号包括时，认为是列
    #                     self.is_join = True
    #                 else:
    #                     try:
    #                         literal = float(literal.strip())
    #                     except ValueError:
    #                         #print(
    #                         #    f"Could not parse literal {literal} (maybe a join condition? if so, this can be ignored)")
    #                         literal = None

    #         assert node_op is not None, f"Could not parse: {self.text}"

    #         self.column = column
    #         if column is not None:
    #             self.column = tuple(column.split('.'))
    #         self.operator = node_op
    #         self.literal = literal
    #         self.filter_feature = filter_feature
    #         self.type = type


def parse_recursively(filter_cond, offset, _class=PredicateNode):
    escaped = False

    node_text = ''
    children = []
    children_offset = []
    while True:
        if offset >= len(filter_cond):
            return _class(node_text, children, children_offset), offset

        if filter_cond[offset] == '(' and not escaped:
            child_node, offset = parse_recursively(filter_cond, offset + 1, _class=_class)
            children.append(child_node)
            children_offset.append(len(node_text))
        elif filter_cond[offset] == ')' and not escaped:
            return _class(node_text, children, children_offset), offset
        elif filter_cond[offset] == "'":
            escaped = not escaped
            node_text += "'"
        else:
            node_text += filter_cond[offset]
        offset += 1


def parse_filter(filter_cond, parse_baseline=False):
    parse_tree, _ = parse_recursively(filter_cond, offset=0)
    assert len(parse_tree.children) == 1
    parse_tree = parse_tree.children[0]
    parse_tree.adjust_tree()
    # parse_tree.parse_lines_recursively(parse_baseline=parse_baseline)
    if parse_tree.check_tree() == False:
        assert False, 'error parse'
    return parse_tree
