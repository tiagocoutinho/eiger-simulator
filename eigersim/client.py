import json

import requests


class Param:

    def __init__(self, addr):
        self.base_addr = addr

    def __set_name__(self, eiger, name):
        self.addr = f'{self.base_addr}/{name}'

    def __get__(self, eiger, type=None):
        return eiger.get_value(self.addr.format(eiger=eiger))

    def __set__(self, eiger, value):
        eiger.put_value(self.addr.format(eiger=eiger), value)


def build_param_type(addr):
    class Par(Param):
        def __init__(self):
            super().__init__(addr)
    return Par


DetectorConfigProperty = build_param_type('detector/api/{eiger.version}/config')


class Eiger:

    def __init__(self, address, zmq=None):
        self.address = address
        self.session = requests.Session()
        self._version = None

    nimages = DetectorConfigProperty()
    frame_time = DetectorConfigProperty()

    @property
    def version(self):
        if self._version is None:
            self._version = self.get_value('detector/api/version/')
        return self._version

    def get(self, addr):
        return self.session.get(f'{self.address}/{addr}')

    def jget(self, addr):
        return self.get(addr).json()

    def get_value(self, addr):
        return self.jget(addr)['value']

    def put(self, addr, data=None):
        return self.session.put(f'{self.address}/{addr}', data=data)

    def jput(self, addr, data=None):
        return self.put(addr, None if data is None else json.dumps(data))

    def put_value(self, addr, data=None):
        return self.jput(addr, None if data is None else dict(value=data))

    def detector_command(self, cmd):
        return self.put(f'detector/api/{self.version}/command/{cmd}')

    def initialize(self):
        return self.detector_command('initialize')

    def arm(self):
        return self.detector_command('arm')

    def disarm(self):
        return self.detector_command('disarm')

    def trigger(self):
        return self.detector_command('trigger')

    def cancel(self):
        return self.detector_command('cancel')

    def abort(self):
        return self.detector_command('abort')

