recostAPI.py 文件是给外部提供的接口，包含解析json得到计划树的函数，以及对计划树重新代价估计的函数

recostMethod.py 文件包含各种节点进行代价估计和基数估计的方法

estimateCost.py 文件包含节点进行代价估计时需要用到的函数, 主要给recostMethod使用

estimateRow.py 文件包含节点进行基数估计时需要用到的函数, 主要给recostMethod使用

utilsel.py 文件包含选择性估计的一些工具函数, 主要给estimateRow使用

util.py 文件包含一些通用工具函数以及节点类的定义


PlanNode 文件夹包含用于存储计划树节点(planNodeAPI & planNodeMethod)的实现，和表达式树节点(filterNode)的实现
