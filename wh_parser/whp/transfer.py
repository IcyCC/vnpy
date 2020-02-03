import os
from wh_parser.whp.node import *
from wh_parser.whp.error import CompileError
import enum
from typing import List, Dict
from .funcat import ExecutionContextStack

class CodeFileType(enum.Enum):
    PY = 'py' # python
    LIB = 'lib' # 
    ST = 'st'


reserved = {
    'CALL': 'CALL',
    'IMPORT': 'IMPORT',
    'WITH': 'WITH',
    'AS': 'AS',
    'AND': 'AND',
    'OR': 'OR'
}

config_list = (
    'AUTOFILTER',
)

trade_func = (
    'BP',
    'BK',
    'SP',
    'SK',
    'BPK',
    'SPK'
)

tokens = [
             'IDENTIFIER',
             'NUMBER',
             'NAMESTR',

             'COLON',
             'DEFINE_NO',
             'DEFINE_ADD',
             'DEFINE_IND',

             'COMMA',
             'SEM',
             'DOT',
             'SHARP',

             'ADD',
             'SUB',
             'MUL',
             'DIV',
             'MOD',

             'LT',
             'GT',
             'EQ',
             'LTE',
             'GTE',
             'NEQ',

             'LPAREN',
             'RPAREN',
             'LBRACKET',
             'RBRACKET',
             'CONFIG',
             'TRADE',
         ] + list(reserved.values())

t_NAMESTR = r'\'[a-zA-Z_0-9]*\''

t_COLON = r':'
t_DEFINE_NO = r':='
t_DEFINE_ADD = r'\^\^'
t_DEFINE_IND = r'\.\.'

t_COMMA = r','
t_SEM = r';'
t_DOT = r'\.'
t_SHARP = r'\#'

t_ADD = r'\+'
t_SUB = r'-'
t_MUL = r'\*'
t_DIV = r'/'
t_MOD = r'%'

t_LT = r'<'
t_GT = r'>'
t_EQ = r'='
t_LTE = r'<='
t_GTE = r'>='
t_NEQ = r'<>'

t_AND = r'&&'
t_OR = r'\|\|'

t_LPAREN = r'\('
t_RPAREN = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'

t_ignore_COMMENT = r'//.*'


def t_IDENTIFIER(t):
    r'[a-zA-Z_]+[a-zA-Z0-9_]*'
    if t.value in config_list:
        t.type = 'CONFIG'
    elif t.value in trade_func:
        t.type = 'TRADE'
    else:
        t.type = reserved.get(t.value, 'IDENTIFIER')
    return t


def t_NUMBER(t):
    r'\d+\.?\d*'
    t.value = float(t.value)
    return t


t_ignore = " \t"


def t_newline(t):
    r'\n+'
    t.lexer.lineno += t.value.count("\n")


def t_error(t):
    raise CompileError(t)


import ply.lex as lex

lex.lex()

precedence = (
    ('left', 'AND', 'OR'),
    ('left', 'LT', 'GT', 'EQ', 'LTE', 'GTE', 'NEQ'),
    ('left', 'ADD', 'SUB'),
    ('left', 'MUL', 'DIV', 'MOD'),
)


def p_program_block(p):
    'program : statements'


def p_statements_statement(p):
    'statements : statement'


def p_statements_statements(p):
    'statements : statements statement'


def p_statement_paramdef(p):
    'statement : IDENTIFIER LBRACKET NUMBER COMMA NUMBER COMMA NUMBER RBRACKET'
    params_dict[p[1]] = (p[3], p[5], p[7])


def p_statement_call(p):
    'statement : SHARP CALL LBRACKET IDENTIFIER COMMA IDENTIFIER RBRACKET AS IDENTIFIER'
    calls_dict[p[9]] = (p[4], p[6])


def p_statement_import(p):
    'statement : SHARP IMPORT LBRACKET IDENTIFIER COMMA NUMBER COMMA IDENTIFIER RBRACKET AS IDENTIFIER'
    import_dict[p[11]] = (p[4], p[6], p[8])


def p_statement_with(p):
    'statement : SHARP WITH LBRACKET IDENTIFIER  RBRACKET AS IDENTIFIER'
    with_dict[p[7]] = (p[4])


def p_statement_varassign(p):
    """statement : IDENTIFIER COLON expression SEM
                 | IDENTIFIER DEFINE_NO expression SEM
                 | IDENTIFIER DEFINE_ADD expression SEM
                 | IDENTIFIER DEFINE_IND expression SEM

    """
    vars_dict[p[1]] = p[3]


def p_statement_condition(pre):
    '''statement : expression COMMA tradecom SEM'''
    conditions.append(ConditionSTMT(indicator=cur_indicator_name, exp=pre[1], body=pre[3]))


def p_statement_config(p):
    'statement : CONFIG SEM'
    settings[p[1]] = True


def p_expression_binop(p):
    '''expression : expression LT  expression
                  | expression GT  expression
                  | expression EQ  expression
                  | expression LTE expression
                  | expression GTE expression
                  | expression NEQ expression
                  | expression ADD expression
                  | expression SUB expression
                  | expression MUL expression
                  | expression DIV expression
                  | expression MOD expression
                  | expression AND expression
                  | expression OR  expression'''
    p[0] = BinaryOperationEXPR(p[1], p[2], p[3])


def p_expression_group(p):
    'expression : LPAREN expression RPAREN'
    p[0] = p[2]


def p_expression_number(p):
    'expression : NUMBER'
    p[0] = NumberEXPR(p[1])


def p_expression_id(p):
    'expression : IDENTIFIER'
    p[0] = IdentifierEXPR(p[1], cur_indicator_name)


def p_expression_future(p):
    'expression : IDENTIFIER DOT IDENTIFIER'
    p[0] = FutureExpr(cur_indicator_name, p[1], p[3])


def p_tradecom_trade(p):
    '''tradecom :
                | TRADE
                | TRADE LPAREN argslist RPAREN'''
    if len(p) > 2:
        p[0] = TradeIdentifierEXPR(p[1], p[3])
    else:
        p[0] = TradeIdentifierEXPR(p[1], list())


def p_expression_methodcall(p):
    'expression : IDENTIFIER LPAREN argslist RPAREN'
    p[0] = MethodCallEXPR(p[1], p[3])


def p_argslist_arg(p):
    '''argslist :
                | expression
                | argslist COMMA expression'''
    if len(p) > 2:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]


def p_error(p):
    raise CompileError(p)


import ply.yacc as yacc

yacc.yacc(start='program')


def conditions_parser(conds: typing.List[ConditionSTMT]):
    """
    将条件单解析成函数
    :param conds: 条件 ConditionSTMT
    :return:
    """

    def conditions_func(lvars: dict):
        for cond in conds:
            c = cond.exp.compile().compute(lvars)
            if c:
                cond.body.compile().compute(lvars)

    return conditions_func


def setting_parser(sets: dict):
    """
    将条件单解析成函数
    :param conds: 条件 ConditionSTMT
    :return:
    """

    def conditions_func(context):
        for k, v in sets.items():
            setattr(context, k, v)

    return conditions_func


def _get_origin_node(name, code, code_type=CodeFileType.LIB):
    if code_type == CodeFileType.LIB or code_type == CodeFileType.ST:
        global cur_indicator_name
        cur_indicator_name = name
        yacc.parse(code)
        global_indicators_dict[name] = IndicatorNode(name=name,
                                                     ctx=global_ctx,
                                                     api_env=global_api_env,
                                                     params=params_dict.copy(),
                                                     calls=calls_dict.copy(),
                                                     imports=import_dict.copy(),
                                                     vars=vars_dict.copy(),
                                                     withs=with_dict.copy(),
                                                     cond_func=conditions_parser(conditions),
                                                     set_func=setting_parser(settings)
                                                     )
        reset_indicator_var()
        return global_indicators_dict[name].compile()
    elif CodeFileType.PY:
        l_vars = {}
        exec(code, l_vars)
        global_indicators_dict[name] = l_vars['get_indicator'](global_ctx)
        reset_indicator_var()
        return global_indicators_dict[name].compile()
    else:
        raise NotImplementedError("不支持的文件类型")


def _parse4single(path: str):
    """
    通过路径单独编译一个文件
    :param path:
    :return:
    """
    name, ext = os.path.splitext(os.path.split(path)[1])
    body = ''
    file_type = CodeFileType.LIB
    if ext == '.lib':
        with open(path, 'r') as f:
            body = f.read()
            file_type = CodeFileType.LIB
    elif  ext = '.st':
        with open(path, 'r') as f:
            body = f.read()
        file_type = CodeFileType.ST

    elif ext == '.py':
        with open(path, 'r') as f:
            body = f.read()
        file_type = CodeFileType.PY
    else:
        NotImplementedError("不支持的文件类型")
    return _get_origin_node(name, f.read(), file_type)

def parse(ctx:ExecutionContextStack,  api_env ,mpath, lib_paths):
    """
    编译代码文件列表
    :param paths:
    :return:
    """
    global_api_env = api_env
    global_ctx = ctx

    for path in lib_paths:
        _parse4single(path)
    main_node = _parse4single(mpath)
    global_indicators_dict.clear()
    settings.clear()
    conditions.clear()
    global_ctx = None
    global_ctx = None
    return main_node
