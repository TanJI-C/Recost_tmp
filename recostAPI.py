from recostNode import *
from recostMethod import *
from utilsel import StatisticInfoOfRel
# 解析json格式的plan为node


def _nodeFactory(json_dict):
    node_type = json_dict['Node Type']
    if node_type == "Nested Loop":
        curr_node = NestLoopNode(json_dict)
        curr_node.recost_fun = lambda: nestloop_info(curr_node, curr_node.children[0], curr_node.children[1])
    elif node_type == "Hash Join":
        curr_node = HashJoinNode(json_dict)
        curr_node.recost_fun = lambda: hashjoin_info(curr_node, curr_node.children[0], curr_node.children[1])
    elif node_type == "Merge Join":
        curr_node = MergeJoinNode(json_dict)
        curr_node.recost_fun = lambda: mergejoin_info(curr_node, curr_node.children[0], curr_node.children[1])
    # elif node_type == "Sort":
    #     pass
        
    return curr_node
    
def ParsePostgresPlanJson(json_dict):
    """Takes JSON dict, parses into a Node."""
    curr = json_dict['Plan']

    def _parse_pg(json_dict):
        curr_node = _nodeFactory(json_dict)

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
        
        curr_node.initAfterSonInit(json_dict)
        # if op == 'Bitmap Heap Scan':
        #     for c in curr_node.children:
        #         if c.node_type == 'Bitmap Index Scan':
        #             # 'Bitmap Index Scan' doesn't have the field 'Relation Name'.
        #             c.table_name = curr_node.table_name
        #             c.table_alias = curr_node.table_alias

        return curr_node

    return _parse_pg(curr)

# TODO: 存储整颗计划树的节点
def StorePlanTreeNode(root):
    pass

def Recost(root: DerivedPlanNode):
    
    cost_list = [0]
    row_list = [0]
    for subplan in root.children:
        sub_cost_list, sub_row_list = Recost(subplan)
        cost_list.extend(sub_cost_list)
        row_list.extend(sub_row_list)
    root.recost_fun()
    cost_list[0] = root.total_cost
    row_list[0] = root.rows
    return cost_list, row_list


def InitStatisticInfo():
    global StatisticInfoOfRel
    