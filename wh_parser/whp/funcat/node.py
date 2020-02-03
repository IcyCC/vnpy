import abc
from wh_parser import funcat
from collections import defaultdict
from .time_series import TimeSeries

class BaseNode(metaclass=abc.ABCMeta):
    """
    基本的节点 使用compute获得值
    """

    @abc.abstractmethod
    def compute(self, pvars=None):
        raise NotImplementedError

    @abc.abstractmethod
    def output(self):
        raise NotImplementedError


class BaseDataNode(BaseNode):
    """
    类似Open Close等基本值
    """

    def __init__(self, value:TimeSeries):
        self.value = value

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        return self.value()

    def output(self):
        return str(self.value.__name__)


class ConstNode(BaseNode):
    """
    常数等值
    """

    def __init__(self, num):
        self.num = num

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        return self.num

    def output(self):
        return str(self.num)


class VarNode(BaseNode):
    """
    维度 需要一个
    """

    def __init__(self, name, plot, node: BaseNode):
        self.name = name
        self.node = node
        self.plot = plot

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        res = self.node.compute(pvars)
        if res is None:
            raise Exception
        pvars[self.name] = res
        return res

    def output(self):
        return self.name + ':' + self.node.output()


class OperatorNode(BaseNode):
    """
    运算符node
    """

    def __init__(self, op, left_node, right_node):
        self.op = op
        self.left_node = left_node
        self.right_node = right_node

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        if self.op == '+':
            return self.left_node.compute(pvars) + self.right_node.compute(pvars)
        elif self.op == '-':
            return self.left_node.compute(pvars) - self.right_node.compute(pvars)
        elif self.op == '*':
            return self.left_node.compute(pvars) * self.right_node.compute(pvars)
        elif self.op == '/':
            return self.left_node.compute(pvars) / self.right_node.compute(pvars)
        elif self.op == '&&' or self.op == 'AND':
            return self.left_node.compute(pvars) & self.right_node.compute(pvars)
        elif self.op == '||' or self.op == 'OR':
            return self.left_node.compute(pvars) | self.right_node.compute(pvars)
        elif self.op == '<':
            return self.left_node.compute(pvars) < self.right_node.compute(pvars)
        elif self.op == '>':
            return self.left_node.compute(pvars) > self.right_node.compute(pvars)
        elif self.op == '<=':
            return self.left_node.compute(pvars) <= self.right_node.compute(pvars)
        elif self.op == '>=':
            return self.left_node.compute(pvars) >= self.right_node.compute(pvars)
        elif self.op == '=':
            return self.left_node.compute(pvars) == self.right_node.compute(pvars)
        elif self.op == '<>':
            return self.left_node.compute(pvars) != self.right_node.compute(pvars)

    def output(self):
        return self.left_node.output() + self.op + self.right_node.output()


class FuncNode(BaseNode):
    """
    函数node
    """

    def __init__(self, func, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.func = func

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        value = list([v.compute(pvars) for v in self.args])
        return self.func(*self.args,**kwargs)

    def output(self):
        return '{name}({args})'.format(name=self.name, args=[i.output() for i in self.args])


class FutureNode(BaseNode):
    """
    获取局部作用域的 跨周期引入的变量的值
    """

    def __init__(self, indicator, future, name):
        self.indicator = indicator
        self.name = name
        self.future = future

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        res = pvars[self.future][self.name]
        return res

    def output(self):
        return '{indicator}.{name}'.format(indicator=self.indicator, name=self.name)


class ArgsNode(BaseNode):
    """
    获取局部作用域的 参数的值
    """

    def __init__(self, name, indicator):
        self.indicator = indicator
        self.name = name

    def compute(self, pvars=None):
        if pvars is None:
            pvars = {}
        res = pvars[self.name]
        if res is None:
            raise Exception
        return res

    def output(self):
        return self.name


class Indicator(object):
    """
    指标基类 作用是传入一组维度 compute返回值
    """

    def __init__(self, name, ctx, api_env, alias, diff=None, deps=None, var_nodes=None, params=None, cond_func=None, set_func=None):
        if var_nodes is None:
            var_nodes = []
        if diff is None:
            diff = {}
        if deps is None:
            deps = {}
        if var_nodes is None:
            var_nodes = []
        if params is None:
            params = {}
        self.name = name
        self.alias = alias
        self.diff = diff
        self.var_nodes = var_nodes
        self.deps = deps
        self.params = params
        self.lvars = defaultdict(dict)
        self.ctx = ctx
        self.api_env = api_env

        if cond_func is None:
            cond_func = lambda v: True
        if set_func is None:
            set_func = lambda v: True

        self._cond_func = cond_func
        self._set_func = set_func

    def call_cond(self):
        """
        处理条件单
        :return:
        """
        self._cond_func(self.lvars)

    def call_setting(self, context):
        """
        处理设置
        :param context:
        :return:
        """
        self._set_func(context)

    def update_params(self, pvars=None):
        """
        更新参数到pvars中
        :return:
        """
        if pvars is None:
            pvars = {}
        for k, v in self.params.items():
            value = v.get("value")
            if value is None:
                value = v["range"][2]
            elif value > v["range"][0] or value < v["range"][1]:
                raise Exception("参数范围不正确")
            pvars[k] = value

    def compute(self, pvars: dict = None):
        if pvars is None:
            pvars = {}
        pvars[self.alias] = defaultdict(dict)
        self.lvars = pvars[self.alias]

        self.update_params(self.lvars)
        with self.ctx.get_active().diff(diff=self.diff):
            for dep_name, dep in self.deps.items():
                dep.compute(self.lvars)

            res = {v.name: v.compute(self.lvars) for v in self.var_nodes}
        return res

    def output(self):
        print("Indicator", self.name)
        for v in self.var_nodes:
            print(v.output())


def ctx_import(freq, num):
    # funcat.ExecutionContext().ge
    f = 'm'
    if freq == 'DAY':
        f = 'd'
    if freq == 'MIN':
        f = 'm'

    return {'freq': str(int(num)) + f}


def ctx_call(code):
    return {"order_book_id": code}


from .cnode import *
