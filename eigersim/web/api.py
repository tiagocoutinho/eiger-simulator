import enum
import json
import functools
from typing import List

import fastapi
import zmq.asyncio


class Param(str, enum.Enum):
    count_time = 'count_time'
    frame_time = 'frame_time'
    description = 'description'
    detector_instance = 'detector_instance'


def Value(value, value_type='string', access_mode='rw', unit='', **kwargs):
    return dict(value=value, value_type=value_type,
                access_mode=access_mode, unit=unit, **kwargs)


def Bool(value, access_mode='rw', **kwargs):
    return Value(value, value_type='bool', access_mode=access_mode, **kwargs)


def BoolR(value, **kwargs):
    return Bool(value, access_mode='r', **kwargs)


def Float(value, min=0., max=0., unit='', access_mode='rw', **kwargs):
    return Value(value, value_type='float', access_mode=access_mode,
                 min=min, max=max, unit=unit, **kwargs)


def FloatR(value, min=0., max=0., unit='', **kwargs):
    return Float(value, min, max, unit, access_mode='r', **kwargs)


def Int(value, min=0, max=0, unit='', access_mode='rw', **kwargs):
    return Value(value, value_type='int', access_mode=access_mode,
                 min=min, max=max, unit=unit, **kwargs)


def IntR(value, min=0, max=0, unit='', **kwargs):
    return Int(value, min, max, unit, access_mode='r', **kwargs)


def Str(value, access_mode='rw', **kwargs):
    return Value(value, value_type='string', access_mode=access_mode, **kwargs)


def StrR(value, **kwargs):
    return Str(value, access_mode='r', **kwargs)


def LStr(value, access_mode='rw', **kwargs):
    return Value(value, value_type='string[]', access_mode=access_mode, **kwargs)


def LStrR(value, **kwargs):
    return LStr(value, access_mode='r', **kwargs)


class Detector:

    def __init__(self, zmq_bind='tcp://0:9999'):
        self.zmq_bind = zmq_bind
        self.zmq_context = zmq.asyncio.Context()
        self.zmq_sock = self.zmq_context.socket(zmq.PUSH)
        self.zmq_sock.bind(self.zmq_bind)

        self.config = {
            'auto_summation': Bool(True),
            'beam_center_x': Float(0.0, 0.0, 1e6),
            'beam_center_y': Float(0.0, 0.0, 1e6),
            'bit_depth_image': IntR(16),
            'bit_depth_readout': IntR(16),
            'chi_increment': Float(0.0, -100, 100),
            'chi_start': Float(0.0, -180, 180),
            'compression': Str('lz4', allowed_values=['lz4', 'bslz4']),
            'count_time': Float(0.5, 0.0000029, 1800, 's'),
            'countrate_correction_applied': Bool(True),
            'countrate_correction_count_cutoff': IntR(0),
            'data_collection_date': Str(''),
            'description': StrR('Eiger9M'),
            'detector_number': StrR('XT67793G1'),
            'detector_distance': Float(1.0, min=0.0, max=1e6),
            'detector_readout_time': FloatR(1e-5, min=0, max=1e6),
            'element': Str('Cu'),
            'flatfield': 'TODO',
            'flatfield_correction_applied': Bool(True),
            'frame_time': Float(1.0, min=1/500, max=1e6, unit='s'),
            'kappa_increment': Float(0.0, -100, 100),
            'kappa_start': Float(0.0, -180, 180),
            'nimages': Int(1, min=1, max=1_000_000),
            'ntrigger': Int(1, min=1, max=1_000_000),
            'number_of_excluded_pixels': IntR(0),
            'omega_increment': Float(0.0, -100, 100),
            'omega_start': Float(0.0, -180, 180),
            'phi_increment': Float(0.0, -100, 100),
            'phi_start': Float(0.0, -180, 180),
            'photon_energy': Float(1.0, min=0, max=1_000_000, unit='eV'),
            'pixel_mask': 'TODO',
            'pixel_mask_applied': Bool(True),
            'roi_mode': Str(''),
            'sensor_material': StrR('CdTe'),
            'sensor_thickness': FloatR(0.001),
            'software_version': StrR('1.6.0'),
            'threshold_energy': Float(1.0),
            'trigger_mode': Str('software'),
            'two_theta_increment': Float(0.0, -100, 100),
            'two_theta_start': Float(0.0, -180, 180),
            'wavelength': Float(1.0),
            'x_pixel_size': FloatR(0.002),
            'y_pixel_size': FloatR(0.001),
            'x_pixels_in_detector': IntR(3840),
            'y_pixels_in_detector': IntR(2160),
        }
        self.status = {}
        self.stream = {
            'config': {
                'mode': Str('enabled'),
                'header_detail': Str('basic'),  # all, basic, none
                'header_appendix': Str(''),
                'image_appendix': Str(''),
            },
            'status': {
                'state': StrR('ready'),
                'error': LStrR([]),
                'dropped': IntR(0)
            }
        }
        self.system = {
            'status': {
            }
        }
        self.series = 0

    async def arm(self):
        now = datetime.datetime.utcnow()
        self.series += 1
        self.stream['status']['state']['value'] = 'armed'
        self.stream['status']['dropped']['value'] = 0
        self.config['data_collection_date'] = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        if self.stream['config']['mode'] == 'enabled':
            await self.send_global_header_data(series=self.series)

    async def disarm(self):
        await self.send_end_of_series(self.series)

    async def cancel(self):
        await self.send_end_of_series(self.series)

    async def abort(self):
        await self.send_end_of_series(self.series)

    async def send_global_header_data(self, series):
        detail = self.stream['config']['header_detail']
        header = dict(htype='dheader-1.0', series=series, header_detail=detail)
        parts = [
            json.dumps(header).encode()
        ]
        if detail in ('basic', 'all'):
            config_header = dict(self.config)
            pixel_mask = config_header.pop('pixel_mask')
            flatfield = config_header.pop('flatfield')
            parts.append(json.dumps(config_header).encode())
        if detail == 'all':
            flatfield_header = dict(htype='dflatfield-1.0',
                                    shape=(100, 100), type='float32')
            parts.append(json.dumps(flatfield_header).encode())
            parts.append(b'flatfield-data-blob-here')
            pixelmask_header = dict(htype='dpixelmask-1.0',
                                    shape=(100, 100), type='uint32')
            parts.append(json.dumps(pixelmask_header).encode())
            parts.append(b'pixelmask-data-blob-here')
            countrate_header = dict(htype='dcountrate_table-1.0',
                                    shape=(100, 100), type='float32')
            parts.append(json.dumps(countrate_header).encode())
            parts.append(b'countrate-table-data-blob-here')
        await self.zmq_sock.send_multipart(parts)

    async def send_end_of_series(self, series):
        header = dict(htype='dseries_end-1.0', series=series)
        await self.zmq_sock.send(json.dumps(header).encode())


app = fastapi.FastAPI()


# DETECTOR MODULE =============================================================

# CONFIG task -----------------------------------------------------------------

@app.get('/detector/api/{version}/config/{param}')
def config(version: str, param: str):
    assert version.startswith('1.6')
    return app.detector.config[param]


@app.put('/detector/api/{version}/config/{param}')
def config_put(version: str, param: str, value: str = None) -> List[str]:
    assert version.startswith('1.6')
    app.detector.config[param]['value'] = value
    return ["bit_depth_image", "count_time",
            "countrate_correction_count_cutoff",
            "frame_count_time", "frame_period", "nframes_sum"]


# STATUS task -----------------------------------------------------------------

@app.get('/detector/api/{version}/status/{param}')
def status(version: str, param: Param):
    assert version.startswith('1.6')
    return app.detector.status[param]


# COMMAND task ----------------------------------------------------------------

@app.put('/detector/api/{version}/command/initialize')
async def initialize(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/arm')
async def arm(version: str):
    assert version.startswith('1.6')
    await app.detector.arm()


@app.put('/detector/api/{version}/command/disarm')
async def disarm(version: str):
    assert version.startswith('1.6')
    await app.detector.disarm()


@app.put('/detector/api/{version}/command/trigger')
async def trigger(version: str):
    assert version.startswith('1.6')
    await app.detector.trigger()


@app.put('/detector/api/{version}/command/cancel')
async def cancel(version: str):
    assert version.startswith('1.6')
    await app.detector.cancel()


@app.put('/detector/api/{version}/command/abort')
async def abort(version: str):
    assert version.startswith('1.6')
    await app.detector.abort()


@app.put('/detector/api/{version}/command/status_update')
def status_update(version: str):
    assert version.startswith('1.6')


# FILE WRITER MODULE ==========================================================

@app.get('/filewriter/api/{version}/{module}')
def filewriter(version: str, module: str):
    assert version.startswith('1.6')


# STREAM MODULE ===============================================================

@app.get('/stream/api/{version}/status/{param}')
def stream_status(version: str, param: str):
    assert version.startswith('1.6')
    return app.detector.stream['status'][param]


@app.get('/stream/api/{version}/config/{param}')
def stream_config(version: str, param: str):
    assert version.startswith('1.6')
    return app.detector.stream['config'][param]


@app.put('/stream/api/{version}/config/{param}')
def stream_config_put(version: str, param: str, value):
    assert version.startswith('1.6')
    app.detector.stream['config'][param]['value'] = value


@app.put('/stream/api/{version}/command/initialize')
def stream_initialize(version: str):
    assert version.startswith('1.6')
    raise NotImplementedError


# MONITOR MODULE ==============================================================

@app.get('/monitor/api/{version}/')
def monitor(version: str):
    assert version.startswith('1.6')
    pass


# SYSTEM MODULE ===============================================================

@app.get('/system/api/{version}/status/{param}')
def system_status(version: str, param: str):
    assert version.startswith('1.6')
    return app.detector.system['status'][param]


@app.get('/system/api/{version}/command/restart')
def system_restart(version: str):
    assert version.startswith('1.6')


