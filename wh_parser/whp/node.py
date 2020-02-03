from .funcat.node import *
import copy
import abc

# 单指标赞
params_dict = {}
calls_dict = {}
import_dict = {}
with_dict = {}
vars_dict = {}
cur_indicator_name = ''
settings = {}
conditions = []

global_api_env = None
global_ctx = None

# 全局指标
global_indicators_dict = {}


def reset_indicator_var():
    # 单指标赞
    global params_dict
    global calls_dict
    global import_dict
    global with_dict
    global vars_dict
    global cur_indicator_name
    global settings
    global conditions

    params_dict.clear()
    calls_dict.clear()
    vars_dict.clear()
    import_dict.clear()
    with_dict.clear()
    settings.clear()
    conditions.clear()

    cur_indicator_name = ''

func_tokens = (
    'MA',
    'WMA',
    'EMA',
    'SMA',
    'SUM',
    'ABS',
    'STD',
    'CROSS',
    'REF',
    'MIN',
    'MAX',
    'EVERY',
    'COUNT',
    'HHV',
    'LLV'
)

sp_tokens = ()

base_tokens = ('OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'VOL')


class Node(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def compile(self):
        pass


class IndicatorNode(Node):
    def __init__(self,ctx, api_env , name, params, calls, imports, withs, vars, cond_func, set_func):
        """

        :param name: 指标名称
        :param params: 指标参数
        :param calls:  指标的 跨品种引入
        :param imports: 指标的跨周期引入
        :param withs: 指标的跨指标引入
        :param vars: 指标的变量
        :param cond_func:  条件单函数
        :param set_func:  设置函数
        """
        self.name = name
        self.alias = name
        self.params = params
        self.calls = calls
        self.imports = imports
        self.withs = withs
        self.vars = vars
        self.diff = {}
        self.ctx = ctx
        self.api_env = api_env

        self.deps = {}
        for as_id, (code, name) in self.calls.items():
            self.deps[as_id] = global_indicators_dict[name].cgdiff(diff=ctx_call(code),
                                                                   alias=as_id)

        for as_id, (freq, num, name) in self.imports.items():
            self.deps[as_id] = global_indicators_dict[name].cgdiff(diff=ctx_import(freq,
                                                                                   num),
                                                                   alias=as_id)

        for as_id, (name) in self.withs.items():
            self.deps[as_id] = global_indicators_dict[name].cgdiff(diff={},
                                                                   alias=as_id)

        self._cond_func = cond_func
        self._set_func = set_func

    def cgdiff(self, alias, diff):
        node = copy.deepcopy(self)
        node.diff = diff
        node.alias = alias
        return node

    def compile(self):
        return Indicator(
            name=self.name,
            ctx=self.ctx,
            api_env=self.api_env,
            var_nodes=[VarNode(name, {"load": "line"}, val.compile()) for name, val in self.vars.items()],
            deps={name: val.compile() for name, val in self.deps.items()},
            params={name: {"range": val} for name, val in self.params.items()},
            diff=self.diff,
            alias=self.alias,
            cond_func=self._cond_func,
            set_func=self._set_func
        )


class ConditionSTMT(Node):
    def __init__(self, indicator, exp, body):
        self.exp = exp
        self.body = body
        self.indicator = indicator

    def compile(self):
        pass


class BinaryOperationEXPR(Node):
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

    def compile(self):
        return OperatorNode(self.op, self.left.compile(), self.right.compile())


class GroupEXPR(Node):
    def __init__(self, expr):
        self.expr = expr

    def compile(self):
        return self.expr.compile()


class NumberEXPR(Node):
    def __init__(self, val):
        self.val = val

    def compile(self):
        return ConstNode(self.val)


class IdentifierEXPR(Node):
    def __init__(self, id, indicator):
        self.id = id
        self.indicator = indicator

    def compile(self):
        if self.id in base_tokens:
            return BaseDataNode(getattr(funcat, self.id))
        else:
            return ArgsNode(self.id, self.indicator)


class TradeIdentifierEXPR(Node):

    def __init__(self, name, args):
        self.name = name
        self.args = args

    def compile(self):
        return FuncNode(global_api_env.func(self.name), *self.args)


class FutureExpr(Node):
    def __init__(self, indicator, alis, var):
        self.alis = alis
        self.var = var
        self.indicator = indicator

    def compile(self):
        return FutureNode(self.indicator, self.alis, self.var)


class MethodCallEXPR(Node):
    def __init__(self, name, args):
        self.name = name
        self.args = args

    def compile(self):
        args = [arg.compile() for arg in self.args]
        return FuncNode(global_api_env.func(self.name), *args)
