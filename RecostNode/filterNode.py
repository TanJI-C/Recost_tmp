from enum import Enum
import math
import re
from typing import TypeVar
from Recost_tmp.RecostNode.util import child_prod

estimated_regex = re.compile(
    '\(cost=(?P<est_startup_cost>\d+.\d+)..(?P<est_cost>\d+.\d+) rows=(?P<est_card>\d+) width=(?P<est_width>\d+)\)')
actual_regex = re.compile('\(actual time=(?P<act_startup_cost>\d+.\d+)..(?P<act_time>\d+.\d+) rows=(?P<act_card>\d+)')
op_name_regex = re.compile('->  ([^"(]+)')
workers_planned_regex = re.compile('Workers Planned: (\d+)')
# filter_columns_regex = re.compile('("\S+"."\S+")')
filter_columns_regex = re.compile('([^\(\)\*\+\-\'\= ]+)')
literal_regex = re.compile('(\'[^\']+\'::[^\'\)]+)')


class Operator(Enum):
    NEQ = '!='
    EQ = '='
    LEQ = '<='
    GEQ = '>='
    LIKE = 'LIKE'
    NOT_LIKE = 'NOT LIKE'
    IS_NOT_NULL = 'IS NOT NULL'
    IS_NULL = 'IS NULL'
    IN = 'IN'
    BETWEEN = 'BETWEEN'

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


class PredicateNode:
    def __init__(self, text, children):
        self.text = text
        self.children = children
        self.column = None
        self.operator = None
        self.literal = None
        self.filter_feature = None

    def __str__(self):
        return self.to_tree_rep(depth=0)

    def to_dict(self):
        return dict(
            column=self.column,
            operator=str(self.operator),
            literal=self.literal,
            literal_feature=self.filter_feature,
            children=[c.to_dict() for c in self.children]
        )

    def lookup_columns(self, plan, **kwargs):
        if self.column is not None:
            self.column = plan.lookup_column_id(self.column, **kwargs)
        for c in self.children:
            c.lookup_columns(plan, **kwargs)

    def parse_lines_recursively(self, parse_baseline=False):
        self.parse_lines(parse_baseline=parse_baseline)
        for c in self.children:
            c.parse_lines_recursively(parse_baseline=parse_baseline)
        # remove any children that have no literal
        if parse_baseline:
            self.children = [c for c in self.children if
                             c.operator in {LogicalOperator.AND, LogicalOperator.OR,
                                            Operator.IS_NOT_NULL, Operator.IS_NULL}
                             or c.literal is not None]

    def parse_lines(self, parse_baseline=False):
        keywords = [w.strip() for w in self.text.split(' ') if len(w.strip()) > 0]
        if all([k == 'AND' for k in keywords]):
            self.operator = LogicalOperator.AND
        elif all([k == 'OR' for k in keywords]):
            self.operator = LogicalOperator.OR
        elif len(keywords) == 1 and keywords[0] == 'NOT':
            self.operator = LogicalOperator.NOT
        else:
            repr_op = [
                ('= ANY', Operator.IN),
                ('=', Operator.EQ),
                ('>=', Operator.GEQ),
                ('>', Operator.GEQ),
                ('<=', Operator.LEQ),
                ('<', Operator.LEQ),
                ('<>', Operator.NEQ),
                ('~~', Operator.LIKE),
                ('!~~', Operator.NOT_LIKE),
                ('IS NOT NULL', Operator.IS_NOT_NULL),
                ('IS NULL', Operator.IS_NULL)
            ]
            node_op = None
            literal = None
            column = None
            filter_feature = 0
            for op_rep, op in repr_op:
                split_str = f' {op_rep} '
                self.text = self.text + ' '

                if split_str in self.text:
                    assert node_op is None
                    node_op = op
                    literal = self.text.split(split_str)[1]
                    column = self.text.split(split_str)[0]

                    # dirty hack to cope with substring calls in
                    is_substring = self.text.startswith('"substring')
                    if is_substring:
                        self.children[0] = self.children[0].children[0]

                    # current restriction: filters on aggregations (i.e., having clauses) are not encoded using
                    # individual columns
                    agg_ops = {'sum', 'min', 'max', 'avg', 'count'}
                    is_having = column.lower() in agg_ops or (len(self.children) == 1
                                                              and self.children[0].text in agg_ops)
                    if is_having:
                        column = None
                        self.children = []
                    else:

                        def recursive_inner(n):
                            # column names can be arbitrarily deep, hence find recursively
                            if len(n.children) == 0:
                                return n.text
                            return recursive_inner(n.children[0])

                        # sometimes column is in parantheses
                        if node_op == Operator.IN:
                            literal = self.children[-1].text
                            if len(self.children) == 2:
                                column = self.children[0].text
                            self.children = []
                        elif len(self.children) == 2:
                            literal = self.children[-1].text
                            column = recursive_inner(self)
                            self.children = []
                        elif len(self.children) == 1:
                            column = recursive_inner(self)
                            self.children = []
                        elif len(self.children) == 0:
                            pass
                        else:
                            raise NotImplementedError

                        # column and literal are sometimes swapped
                        type_suffixes = ['::bpchar']
                        if any([column.endswith(ts) for ts in type_suffixes]):
                            tmp = literal
                            literal = column
                            column = tmp.strip()

                        # additional features for special operators
                        # number of values for in operator
                        if node_op == Operator.IN:
                            filter_feature = literal.count(',')
                        # number of wildcards for LIKE
                        elif node_op == Operator.LIKE or node_op == Operator.NOT_LIKE:
                            filter_feature = literal.count('%')

                        break

            if parse_baseline:
                if node_op in {Operator.IS_NULL, Operator.IS_NOT_NULL}:
                    literal = None
                elif node_op == Operator.IN:
                    literal = literal.split('::')[0].strip("'").strip("{}")
                    literal = [c.strip('"') for c in literal.split('",')]
                else:
                    if '::text' in literal:
                        literal = literal.split("'::text")[0].strip("'")
                    elif '::bpchar' in literal:
                        literal = literal.split("'::bpchar")[0].strip("'")
                    elif '::date' in literal:
                        literal = literal.split("'::date")[0].strip("'")
                    elif '::time without time zone' in literal:
                        literal = literal.split("'::time")[0].strip("'")
                    elif '::double precision' in literal:
                        literal = float(literal.split("'::double precision")[0].strip("'"))
                    elif '::numeric' in literal:
                        literal = float(literal.split("'::numeric")[0].strip("'"))
                    elif '::integer' in literal:
                        literal = float(literal.split("'::integer")[0].strip("'"))
                    # column comparison. ignored.
                    elif re.match(r"\D\w*\.\D\w*", literal.replace('"', '').replace('\'', '').strip()):
                        literal = None
                    else:
                        try:
                            literal = float(literal.strip())
                        except ValueError:
                            #print(
                            #    f"Could not parse literal {literal} (maybe a join condition? if so, this can be ignored)")
                            literal = None

            assert node_op is not None, f"Could not parse: {self.text}"

            self.column = column
            if column is not None:
                self.column = tuple(column.split('.'))
            self.operator = node_op
            self.literal = literal
            self.filter_feature = filter_feature

    def to_tree_rep(self, depth=0):
        rep_text = '\n' + ''.join(['\t'] * depth)
        rep_text += self.text

        for c in self.children:
            rep_text += c.to_tree_rep(depth=depth + 1)

        return rep_text

class VarRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        self.column_name = None


class ConstRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        # Const values
        self.val = None

class NotClauseRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        self.arg = None              #去掉Not之后的restrict

class AndClauseRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        self.args = []

class OrClauseRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        self.args = []

class OpExprRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        self.opno = None            # 操作符类型
        self.args = []              # 左右的参数 # 左右的参数分别存储{type(属性还是值), rel_name, col_name, val}
        self.left_rel_array = []   # 如果有两个参数.分别记录左操作数的表
        self.right_rel_array = []  # 和右操作数的表

class DistinctExprRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        self.opno = None
        self.args = []
        self.left_rel_array = []
        self.right_rel_array = []

class ScalarArrayOpExprRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

class RowCompareExprRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

class NullTestRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

class BooleanTestRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

class CurrentOfExprRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

class RelabelTypeRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

class CoerceToDomainRestrict(PredicateNode):
    def __init__(self) -> None:
        super().__init__()
        pass

DerivedPredicateNode = TypeVar('DerivedPredicateNode', bound=PredicateNode)




def parse_recursively(filter_cond, offset, _class=PredicateNode):
    escaped = False

    node_text = ''
    children = []

    while True:
        if offset >= len(filter_cond):
            return _class(node_text, children), offset

        if filter_cond[offset] == '(' and not escaped:
            child_node, offset = parse_recursively(filter_cond, offset + 1, _class=_class)
            children.append(child_node)
        elif filter_cond[offset] == ')' and not escaped:
            return _class(node_text, children), offset
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
    parse_tree.parse_lines_recursively(parse_baseline=parse_baseline)
    if parse_tree.operator not in {LogicalOperator.AND, LogicalOperator.OR, Operator.IS_NOT_NULL, Operator.IS_NULL} \
            and parse_tree.literal is None:
        return None
    return parse_tree
