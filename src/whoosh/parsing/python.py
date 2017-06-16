import ast
import operator


# Allowed operators
ast_to_operator = {
    # Allow +, -, *, /, %, negative:
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.USub: operator.neg,

    # Allow comparison:
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}


# Evaluate a parsed AST tree using a custom namespace object

def ast_eval(ns, node):
    nodetype = type(node)

    if nodetype is ast.Num:
        return node.n

    elif nodetype is ast.Str:
        return node.s

    elif nodetype is ast.Compare:
        left = ast_eval(ns, node.left)
        for comp_op, right_expr in zip(node.ops, node.comparators):
            right = ast_eval(ns, right_expr)
            op = ast_to_operator[type(comp_op)]
            if not op(left, right):
                return False
            left = right

    elif nodetype is ast.BoolOp:
        booltype = type(nodetype.op)
        if booltype is ast.And:
            fn = all
        elif booltype is ast.Or:
            fn = any
        else:
            raise TypeError(node)
        return fn(ast_eval(ns, n) for n in node.values)

    elif nodetype is ast.BinOp:
        op = ast_to_operator[type(node.op)]
        return op(ast_eval(ns, node.left), ast_eval(ns, node.right))

    elif nodetype is ast.UnaryOp:
        op = ast_to_operator[type(node.op)]
        return op(ast_eval(ns, node.operand))

    elif nodetype is ast.Name:
        return getattr(ns, node.id)

    elif nodetype is ast.Call:
        fn = ast_eval(ns, node.func)
        args = [ast_eval(ns, arg) for arg in node.args]
        kwargs = dict([(kw.arg, ast_eval(ns, kw.value))
                       for kw in node.keywords])
        return fn(*args, **kwargs)

    elif nodetype is ast.Attribute:
        target = ast_eval(ns, node.value)
        return getattr(target, node.attr)

    raise TypeError(node)


