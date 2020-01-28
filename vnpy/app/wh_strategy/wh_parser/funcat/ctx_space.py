from wh_parser.funcat.context import ExecutionContext


class CtxSpace:
    """
        拥有一个独立上下文的空间
    """

    def __init__(self, ctx:ExecutionContext=None):
        if ctx is None:
            ctx = ExecutionContext.get_active()
        self.ctx = ctx

    def ctx_wrapper(self, f):
        """
        上下问装饰器
        :param f:
        :return:
        """
        def func(*args, **kwargs):
            self.ctx._push()
            res = f(*args, **kwargs)
            self.ctx._pop()
            return res
        return func

    @classmethod
    def ctx_import(cls, freq, n):
        ctx = ExecutionContext.get_active().copy()
        ctx.set_current_freq(freq)
        return cls(ctx)

    @classmethod
    def ctx_call(cls, code):
        ctx = ExecutionContext.get_active().copy()
        ctx._order_book_id = code
