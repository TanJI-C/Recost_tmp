import Recost_tmp.RecostNode.filterNode as fnode

# Var 无法识别
# print(fnode.parse_filter("(table.a)"))

# Const 无法识别
# print(fnode.parse_filter("(10)"))

# Param skip

# NotClause: 无法识别
print(fnode.parse_filter("(NOT (table.a = 10))"))

# ANDClause: success
# print(fnode.parse_filter("((table.a = 10) AND (table2.b = 1))"))
# print(fnode.parse_filter("((table.a = 10) AND (table2.b = 1) AND (table3.c = 1))"))

# ORClause: success
# print(fnode.parse_filter("((table.a = 10) OR (table2.b = 1))"))

# OpExpr: success(= <> >= > <= < ~~ !~~)
# print(fnode.parse_filter("(table.a = 10)"))

# DistinctExpr: 无法识别
# print(fnode.parse_filter("(table.a IS DISTINCT FROM 10)"))

# ScalarArrayOpExpr: = ANY()success, = ALL()无法识别
# print(fnode.parse_filter("(number_column = ANY ('{1,2,3}'::integer[]))"))

# RowCompareExpr: skip

# NullTest: success
# print(fnode.parse_filter("(table.a IS NULL)"))

# BooleanTest: 无法识别
# print(fnode.parse_filter("(table.a IS TRUE)"))

# CurrentOfExpr: skip

# RelabelType: skip

# CoerceTODomain: skip