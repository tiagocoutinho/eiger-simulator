import json

import requests

from . import config


class Param:

    def __init__(self, addr, name):
        self.base_addr = addr
        self.addr = f'{addr}/{name}'

    def __set_name__(self, eiger, name):
        self.addr = f'{self.base_addr}/{name}'

    def __get__(self, eiger, type=None):
        return eiger.get_value(self.addr.format(eiger=eiger))

    def __set__(self, eiger, value):
        eiger.put_value(self.addr.format(eiger=eiger), value)


def ParamType(addr):
    class Par(Param):
        def __init__(self, name):
            super().__init__(addr, name)
    return Par


DetectorConfigProperty = ParamType('detector/api/{eiger.version}/config')
DetectorStatusProperty = ParamType('detector/api/{eiger.version}/status')
MonitorConfigProperty = ParamType('monitor/api/{eiger.version}/config')
MonitorStatusProperty = ParamType('monitor/api/{eiger.version}/status')
StreamConfigProperty = ParamType('stream/api/{eiger.version}/config')
StreamStatusProperty = ParamType('stream/api/{eiger.version}/status')
SystemStatusProperty = ParamType('system/api/{eiger.version}/status')
FileWriterConfigProperty = ParamType('filewriter/api/{eiger.version}/config')


class BaseModule:
    def __init__(self, eiger):
        self._eiger = eiger
    def __getattr__(self, name):
        return getattr(self._eiger, name)


def Module(*class_configs, Mod=None):
    if Mod is None:
        class Mod(BaseModule):
            pass
    for klass, config in zip(class_configs[::2], class_configs[1::2]):
        for name in config:
            setattr(Mod, name, klass(name))
    return Mod


class Eiger:

    Monitor = Module(MonitorConfigProperty, config.GEN_MONITOR_CONFIG,
                     MonitorStatusProperty, config.GEN_MONITOR_STATUS)
    Stream = Module(StreamConfigProperty, config.GEN_STREAM_CONFIG,
                    StreamStatusProperty, config.GEN_STREAM_STATUS)
    System = Module(SystemStatusProperty, config.GEN_SYSTEM_STATUS)
    FileWriter = Module(FileWriterConfigProperty, config.GEN_FILEWRITER_CONFIG)

    def __init__(self, address, zmq=None):
        self._version = None
        self.address = address
        self.session = requests.Session()
        self.monitor = self.Monitor(self)
        self.stream = self.Stream(self)
        self.system = self.System(self)
        self.filewriter = self.FileWriter(self)

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

    def __repr__(self):
        return f'Eiger({self.address})'

# Export detector properties (nimages, frame_time, etc) directly
# in the Eiger object instead of a detector element
Module(DetectorConfigProperty, config.GEN_DETECTOR_CONFIG,
       DetectorStatusProperty, config.GEN_DETECTOR_STATUS, Mod=Eiger)

