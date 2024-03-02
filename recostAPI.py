from Recost_tmp.PlanNode.planNodeMethod import nodeFactory
from Recost_tmp.statisticInfo import init_statistic
# 解析json格式的plan为node



def RecostInit():
    init_statistic()
    
def ParsePostgresPlanJson(json_dict):
    """Takes JSON dict, parses into a Node."""
    curr = json_dict['Plan']

    def _parse_pg(json_dict):
        curr_node = nodeFactory(json_dict)

        # if op == 'Aggregate':
        #     op = json_dict['Partial Mode'] + op
        #     if select_exprs is None:
        #         # Record the SELECT <select_exprs> at the topmost Aggregate.
        #         # E.g., ['min(mi.info)', 'min(miidx.info)', 'min(t.title)'].
        #         select_exprs = json_dict['Output']
        
        # if 'Relation Name' in json_dict:
        #     curr_node.table_name = json_dict['Relation Name']
        #     curr_node.table_alias = json_dict['Alias']

        # if 'Scan' in op and select_exprs:
        #     # Record select exprs that belong to this leaf.
        #     # Assume: SELECT <exprs> are all expressed in terms of aliases.
        #     if 'Alias' in json_dict.keys():
        #         filtered = _FilterExprsByAlias(select_exprs, json_dict['Alias'])
        #         if filtered:
        #             curr_node.info['select_exprs'] = filtered

        # Recurse.
        if 'Plans' in json_dict:
            for subplan_json in json_dict['Plans']:
                curr_node.children.append(_parse_pg(subplan_json))

        
        curr_node.init_after_son_init()

        return curr_node

    return _parse_pg(curr)

# TODO: 存储整颗计划树的节点
def StorePlanTreeNode(root):
    pass

# 修改_p函数的参数值
def UpdateParamOfPlanTree(root, dict):
    root.update_param

def Recost(root):
    
    cost_list = [0]
    row_list = [0]
    for subplan in root.children:
        sub_cost_list, sub_row_list = Recost(subplan)
        cost_list.extend(sub_cost_list)
        row_list.extend(sub_row_list)
    root.recost_fun()
    cost_list[0] = root.total_cost
    row_list[0] = root.est_rows
    return cost_list, row_list
    