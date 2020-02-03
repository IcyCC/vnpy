import abc
import os
from multiprocessing import Process, Queue


class AbcDataPipeline(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def execute(self):
        raise NotImplementedError


class TaskPipelineBase(AbcDataPipeline):
    name = '单任务处理'

    def __init__(self, in_data_path, out_data_path, process_num=4):
        self.in_data_path = in_data_path
        self.out_data_path = out_data_path
        self.process_num = process_num
        self._queue = Queue()

    @abc.abstractmethod
    def do_task(self, task):
        raise NotImplementedError

    def do_task_process_wrapper(self):
        while True:
            task = self._queue.get()
            if task is None:
                break
            print("处理任务 {}".format(task))
            self.do_task(task)

    @abc.abstractmethod
    def task_gen(self):
        raise NotImplementedError

    def execute(self):
        print("模块: {} 数据处理开始".format(self.name))
        precesses = [Process(target=self.do_task_process_wrapper) for i in range(0, self.process_num)]
        [p.start() for p in precesses]

        for task in self.task_gen():
            self._queue.put(task)
        for i in range(0, self.process_num):
            self._queue.put(None)

        [p.join() for p in precesses]
        print("模块: {} 数据处理完成".format(self.name))


class SingleDataPipelineBase(TaskPipelineBase):
    name = '单文件处理'

    def __init__(self, in_data_path, out_data_path, process_num=4):
        super().__init__(in_data_path, out_data_path, process_num)

    def task_gen(self):
        for root, dirs, files in os.walk(os.path.join(self.in_data_path)):
            for file in files:
                if file.endswith('.csv'):
                    yield os.path.join(root, file)


from .logic_time import LogicTime
from .logprice import LogPrice
from .move_data import MoveData
from .splite_instruments import SplitInstruments
from .main_instrument import MainInstrument
