from wh_parser.funcat.node import BaseNode

class TestNode(BaseNode):

    def output(self):
        return ""

    def compute(self, pvars):
        return 1+2

