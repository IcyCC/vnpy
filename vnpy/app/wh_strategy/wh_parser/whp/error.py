class CompileError(Exception):
    def __init__(self, error):
        """
        业务逻辑的错误
        :param code:  逻辑错误码
        :param http_code:  http状态码
        :param err_info:  文字的错误信息
        """
        super().__init__(self)
        self.error = error

    def __str__(self):
        return "Syntax error at LINE {lineno}, ERROR TYPE {type}, ERROR VALUE: {value}".format(lineno=self.error.lineno,
                                                                                      type=self.error.type,
                                                                                      value=self.error.value)
