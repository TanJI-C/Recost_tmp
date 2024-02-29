import Recost_tmp.PlanNode.filterNode as fnode
import Recost_tmp.PlanNode.planNodeAPI as rNode

# Var 无法识别
# print(fnode.parse_filter("(table.a)"))

# Const 无法识别
# print(fnode.parse_filter("(10)"))

# Param skip

# NotClause: success(change)
# print(fnode.parse_filter("(NOT (table.a = 10))"))

# ANDClause: success
# print(fnode.parse_filter("((table.a = 10) AND (table2.b = 1))"))
# print(fnode.parse_filter("((table.a = 10) AND (table2.b = 1) AND (table3.c = 1))"))

# ORClause: success
# print(fnode.parse_filter("((table.a = 10) OR (table2.b = 1))"))

# OpExpr: success(= <> >= > <= < ~~ !~~)
# print(fnode.parse_filter("(table.a = 10)"))

# DistinctExpr: success
# print(fnode.parse_filter("(table.a IS DISTINCT FROM 10)"))

# ScalarArrayOpExpr: = ANY()success, = ALL() skip
# print(fnode.parse_filter("((number_column)::numeric = ANY ('{1.5,2.7,3.2}'::numeric[]))"))

# RowCompareExpr: skip

# NullTest: success
# print(fnode.parse_filter("(table.a IS NULL)"))

# BooleanTest: success(change)
# print(fnode.parse_filter("(table.a IS TRUE)"))

# CurrentOfExpr: skip

# RelabelType: skip

# CoerceTODomain: skip


# _p函数: success
# print(fnode.parse_filter("((table.b = 10) AND (table.a = _p(123)))"))
# ((4 + 5) * 2 - (3 + 1) / 2)

# print(fnode.parse_filter("((_p(10))::numeric = ((((number_column)::bpchar)::numeric + '2'::numeric) * '3'::numeric))"))
# print(fnode.parse_filter("('10'::numeric = ((((number_column)::bpchar)::numeric + '2'::numeric) * ((3 + number_column))::numeric))"))
# explain (FORMAT json, ANALYZE, VERBOSE) select * from table1 where number_column = _p(123)
# print(fnode.parse_filter("(\"substring\"((string_column)::text, 2, 3) = 'ell'::text)"))



# import json 
# from Recost_tmp.PlanNode.planNodeMethod import nodeFactory
# from Recost_tmp.recostAPI import *
# path = '/home/tanji/Documents/AI4DB/Recost_tmp/test.json'

# # 打开JSON文件并加载数据
# with open(path, 'r') as json_file:
#     data = json.load(json_file)

# # 现在，您可以使用加载的JSON数据进行操作
# # 例如，访问特定的键或执行其他处理

# # 打印加载的JSON数据
# ParsePostgresPlanJson(data)