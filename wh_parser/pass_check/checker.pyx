import os
import pickle
import requests
import uuid
import hashlib
import base64


def get_mac_address():
    node = uuid.getnode()
    mac = uuid.UUID(int=node).hex[-12:]
    return mac


class SeriesChecker(object):
    SERVER = 'http://127.0.0.1:5000/'

    def __init__(self, series_path):
        self._series_path = series_path
        self._series = None
        self.load_series()

    @property
    def series(self):
        return self._series

    def load_series(self):
        """
        加载
        """
        if os.path.exists(self._series_path):
            with open(self._series_path, 'rb') as f:
                self._series = pickle.load(f)

    def save_series(self):
        """
        加载
        """
        with open(self._series_path, 'wb') as f:
            f.write(pickle.dumps(self._series))

    def get_printer(self):
        return str(base64.b32encode(bytes(get_mac_address(), encoding='utf-8')), encoding='utf-8')

    def activate(self, series):
        """

        :return:
        """
        resp = requests.post(url=self.SERVER + 'activate', json={
            "printer": self.get_printer(),
            "series": series
        })
        body = resp.json()
        if body.get("code", -1) == 200:
            self._series = series
            self.save_series()
            return True
        return False

    def check(self):
        # if not self._series:
        #     return False
        # resp = requests.post(url=self.SERVER + 'check', json={
        #     "printer": self.get_printer(),
        #     "series": self._series
        # })
        # body = resp.json()
        # if body.get("code", -1) == 200:
        #     return True
        # else:
        #     self._series = None
        #     self.save_series()
        #     return False
        return True
