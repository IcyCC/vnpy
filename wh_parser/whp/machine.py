from attrdict import AttrDict
from .funcat import ExecutionContext, ExecutionContextStack,registe_sys_data_api, registe_sys_func_api,Indicator
from typing import Callable, Dict
from .transfer import parse
from __future__ import annotations

class FuncatMachine(object):
    
    def __init__(self):
        self._ctx = ExecutionContextStack()


        self._api_env = AttrDict({
            "data": {},
            "func": {}
        })
        registe_sys_data_api(ctx=self._ctx, api_env=self._api_env.data)
        registe_sys_func_api(ctx=self._ctx, api_env=self._api_env.func)

    @property
    def data_api(self) -> Dict:
        """
        数据 api 类似 open
        Returns:
            Dict -- [description]
        """
        return self._api_env.data

    @property
    def func_api(self)-> Dict:
        """
        函数 api
        Returns:
            Dict -- [description]
        """
        return self._api_env.func

    @property
    def ctx(self) -> ExecutionContextStack:
        """上下文
        
        Returns:
            ExecutionContextStack -- [description]
        """
        return self._ctx


    def registe_data_api(self, name:str, callback:Callable[[ExecutionContextStack], object]):
        self.data_api[name] = callback(self.ctx)


    def registe_func_api(self, name:str, callback:Callable[[ExecutionContextStack], object]):
        self.func_api[name] = callback(self.ctx)


    def complie(main_path, lib_paths) -> Indicator:
        """编译
        
        Arguments:
            main_path {[type]} -- [主节点路径]
            lib_paths {[type]} -- [库路径]]
        
        Returns:
            [type] -- [description]
        """
        return parse(self.ctx, self._api_env, main_path, lib_paths)