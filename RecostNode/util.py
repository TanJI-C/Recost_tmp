import math

def child_prod(p, feature_name, default=1):
    child_feat = [c.plan_parameters.get(feature_name) for c in p.children
                  if c.plan_parameters.get(feature_name) is not None]
    if len(child_feat) == 0:
        return default
    return math.prod(child_feat)

