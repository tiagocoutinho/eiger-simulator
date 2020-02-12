import enum
import json
import queue
import asyncio
import logging
import datetime
import functools
import threading

from typing import List

import zmq
import fastapi

from . import config
from .dataset import frames_iter
from .tool import utc, utc_str
from .acquisition import acquire


log = logging.getLogger('eigersim.web')


class Version(str, enum.Enum):
    v1_6_0 = '1.6.0'


class Queue(queue.Queue):

    def flush(self):
        flushed = 0
        while not self.empty():
            self.get_nowait()
            self.task_done()
            flushed += 1
        return flushed

    def __iter__(self):
        while True:
            yield self.get()


class ZMQChannel:

    def __init__(self, address='tcp://*:9999', context=None, timeout=0.1,
                 queue_maxsize=10000):
        self.address = address
        self.context = context or zmq.Context()
        self.sock = None
        self.task = None
        if timeout in (None, 0):
            timeout = 0
        else:
            timeout = -1 if timeout < 0 else int(timeout * 1000)
        self.timeout = timeout
        self.queue = Queue(queue_maxsize)

    def loop(self):
        queue = self.queue
        sock = self.sock
        for parts in queue:
            if parts is None:
                break
            try:
                if len(parts) > 1:
                    sock.send_multipart(parts, copy=False)
                else:
                    sock.send(parts[0], copy=False)
            except zmq.ZMQError as err:
                flushed = queue.flush()
                log.info(f'Error send ZMQ: {err!r}. Flushed {flushed} messages')
            queue.task_done()

    def initialize(self):
        self.close()
        self.sock = self.context.socket(zmq.PUSH)
        self.sock.sndtimeo = self.timeout
        self.sock.bind(self.address)
        self.task = threading.Thread(target=self.loop)
        self.task.daemon = True
        self.task.start()

    def send(self, *parts):
        self.queue.put(parts)

    def close(self):
        self.queue.flush()
        if self.task:
            self.queue.put(None)
            self.task.join()
            self.task = None
        if self.sock:
            log.warn('closing')
            self.sock.close()
            self.sock = None


class Detector:

    def __init__(self, zmq_bind='tcp://0:9999', dataset=None, max_memory=1_000_000_000):
        self.dataset = dataset
        self.max_memory = max_memory
        self.zmq_bind = zmq_bind
        self.zmq = None
        self.config = config.detector_config()
        self.status = config.detector_status()
        self.monitor = {
            'config': config.monitor_config(),
            'status': config.monitor_status()
        }
        self.stream = {
            'config': config.stream_config(),
            'status': config.stream_status()
        }
        self.system = {
            'status': config.system_status()
        }
        self.filewriter = {
            'config': config.filewriter_config()
        }
        self.series = 0
        self.acquisition = None

    async def acquire(self, count_time=None):
        if count_time is None:
            count_time = self.config['count_time']['value']
        nb_frames = self.config['nimages']['value']
        zmq = self.zmq if self.stream_enabled else None
        loop = asyncio.get_running_loop()
        result = loop.run_in_executor(
            None, acquire, count_time, nb_frames, self.series, self.frames, zmq)
        await result

    def _build_dataset(self):
        if self.dataset is None:
            raise NotImplementedError
        else:
            total_size = 0
            frames = []
            for frame in frames_iter(self.dataset):
                total_size += frame.nbytes
                frames.append((zmq.Frame(frame), frame, f'bs{frame.dtype.itemsize * 8}-lz4<'))
                if total_size > self.max_memory:
                    break
            self.frames = frames

    async def initialize(self):
        log.info('[START] initialize')
        log.info('  [START] initialize: build dataset')
        self._build_dataset()
        log.info('  [ END ] initialize: build dataset')
        self.status['state']['value'] = 'initialize'
        log.info('  [START] initialize: ZMQ')
        if self.zmq:
            self.zmq.close()
        self.zmq = ZMQChannel(self.zmq_bind)
        self.zmq.initialize()
        log.info('  [ END ] initialize: ZMQ')
        self.status['state']['value'] = 'ready'
        log.info('[ END ] initialize')

    async def arm(self):
        self.series += 1
        self.stream['status']['state']['value'] = 'armed'
        self.stream['status']['dropped']['value'] = 0
        self.config['data_collection_date']['value'] = utc_str()
        if self.stream['config']['mode']['value'] == 'enabled':
            await self.send_global_header_data(series=self.series)
        return self.series

    async def disarm(self):
        if self.acquisition:
            self.acquisition.cancel()
        return self.series

    async def cancel(self):
        if self.acquisition:
            self.acquisition.cancel()
        return self.series

    async def abort(self):
        if self.acquisition:
            self.acquisition.cancel()
        return self.series

    def trigger(self, count_time=None):
        if self.acquisition:
            raise RuntimeError('Acquisition already in progress')
        self.acquisition = asyncio.create_task(self.acquire(count_time))
        self.acquisition.add_done_callback(self._on_acquisition_finished)
        return self.acquisition

    def _on_acquisition_finished(self, task):
        self.stream['status']['state']['value'] = 'ready'
        self.acquisition = None
        if task.cancelled():
            log.warning('acquisition canceled')
            asyncio.create_task(self.send_end_of_series(self.series))
        else:
            err = task.exception()
            if err:
                log.error(f'acquisition error {err!r}')
            else:
                log.info('acquisition done')
                asyncio.create_task(self.send_end_of_series(self.series))

    async def status_update(self):
        raise NotImplementedError

    @property
    def stream_enabled(self):
        return self.stream['config']['mode']['value'] == 'enabled'

    async def send_global_header_data(self, series):
        if not self.stream_enabled:
            return
        detail = stream_config['header_detail']['value']
        header = dict(htype='dheader-1.0', series=series, header_detail=detail)
        parts = [
            json.dumps(header).encode()
        ]
        if detail in ('basic', 'all'):
            config_header = {k: v['value'] for k, v in self.config.items()}
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
        self.zmq.send(*parts)

    async def send_end_of_series(self, series):
        if not self.stream_enabled:
            return
        header = dict(htype='dseries_end-1.0', series=series)
        self.zmq.send(json.dumps(header).encode())

    async def monitor_clear(self):
        raise NotImplementedError

    async def monitor_initialize(self):
        raise NotImplementedError

    async def filewriter_clear(self):
        raise NotImplementedError

    async def filewriter_initialize(self):
        raise NotImplementedError

    async def stream_initialize(self):
        raise NotImplementedError

    async def system_restart(self):
        raise NotImplementedError


app = fastapi.FastAPI()


# DETECTOR MODULE =============================================================

@app.get('/detector/api/version/')
def version():
    return dict(value='1.6.0', value_type='string')


# CONFIG task -----------------------------------------------------------------

@app.get('/detector/api/{version}/config/{param}')
def detector_config(version: Version, param: str):

    return app.detector.config[param]


@app.put('/detector/api/{version}/config/{param}')
def detector_config_put(version: Version, param: str, body=fastapi.Body(...)) -> List[str]:
    app.detector.config[param]['value'] = body['value']
    return ["bit_depth_image", "count_time",
            "countrate_correction_count_cutoff",
            "frame_count_time", "frame_period", "nframes_sum"]


# STATUS task -----------------------------------------------------------------

@app.get('/detector/api/{version}/status/{param}')
def detector_status(version: Version, param: str):
    return app.detector.status[param]


@app.get('/detector/api/{version}/status/board_000/{param}')
def detector_status_board(version: Version, param: str):
    return app.detector.status['board_000/' + param]


# COMMAND task ----------------------------------------------------------------

@app.put('/detector/api/{version}/command/initialize')
async def initialize(version: Version):
    return await app.detector.initialize()


@app.put('/detector/api/{version}/command/arm')
async def arm(version: Version):
    return {'sequence id': await app.detector.arm()}


@app.put('/detector/api/{version}/command/disarm')
async def disarm(version: Version):
    return {'sequence id': await app.detector.disarm()}


@app.put('/detector/api/{version}/command/trigger')
async def trigger(version: Version, count_time: float = None):
    await app.detector.trigger(count_time)


@app.put('/detector/api/{version}/command/cancel')
async def cancel(version: Version):
    return {'sequence id': await app.detector.cancel()}


@app.put('/detector/api/{version}/command/abort')
async def abort(version: Version):
    return {'sequence id': await app.detector.abort()}


@app.put('/detector/api/{version}/command/status_update')
async def status_update(version: Version):
    return await app.detector.status_update()


# MONITOR MODULE ==============================================================

@app.get('/monitor/api/{version}/config/{param}')
def monitor_config(version: Version, param: str):
    return app.detector.monitor['config'][param]


@app.put('/monitor/api/{version}/config/{param}')
def monitor_config_put(version: Version, param: str, body=fastapi.Body(...)):
    app.detector.monitor['config'][param]['value'] = body['value']


@app.get('/monitor/api/{version}/images')
def images(version: Version, param: str):
    raise NotImplementedError


@app.get('/monitor/api/{version}/images/{series}/{image}')
def image(version: Version, series: int, image: int):
    # Return image in TIFF format
    raise NotImplementedError


@app.get('/monitor/api/{version}/images/monitor')
def last_image(version: Version):
    # Return last image in TIFF format
    raise NotImplementedError


@app.get('/monitor/api/{version}/images/next')
def consume_image(version: Version):
    # Consume first image in TIFF format
    raise NotImplementedError


@app.get('/monitor/api/{version}/status/{param}')
def consume_image(version: Version, param: str):
    return app.detector.monitor['status'][param]


@app.put('/monitor/api/{version}/command/clear')
async def monitor_clear(version: Version):
    await app.detector.monitor_clear()


@app.put('/monitor/api/{version}/command/initialize')
async def monitor_initialize(version: Version):
    await app.detector.monitor_initialize()


# FILE WRITER MODULE ==========================================================

@app.get('/filewriter/api/{version}/config/{param}')
def filewriter_config(version: Version, param: str):
    return app.detector.filewriter['config'][param]


@app.put('/filewriter/api/{version}/config/{param}')
def filewriter_config_put(version: Version, param: str, body=fastapi.Body(...)):
    app.detector.filewriter['config'][param]['value'] = body['value']


@app.get('/filewriter/api/{version}/status/{param}')
def filewriter_config(version: Version, param: str):
    return app.detector.filewriter['status'][param]


@app.get('/filewriter/api/{version}/files')
def file_list(version: Version):
    raise NotImplementedError


@app.put('/filewriter/api/{version}/command/clear')
async def filewriter_clear(version: Version):
    return await app.detector.filewriter_clear()


@app.put('/filewriter/api/{version}/command/initialize')
async def filewriter_initialize(version: Version):
    return await app.detector.filewriter_initialize()


# DATA MODULE =================================================================

@app.get('/data/{pattern}_master.h5')
def master_file(version: Version, pattern: str):
    raise NotImplementedError


@app.get('/data/{pattern}_data_{file_nb}.h5')
def data_file(version: Version, pattern: str, file_nb: int):
    raise NotImplementedError



# STREAM MODULE ===============================================================

@app.get('/stream/api/{version}/status/{param}')
def stream_status(version: Version, param: str):
    return app.detector.stream['status'][param]


@app.get('/stream/api/{version}/config/{param}')
def stream_config(version: Version, param: str):
    return app.detector.stream['config'][param]


@app.put('/stream/api/{version}/config/{param}')
def stream_config_put(version: Version, param: str, body=fastapi.Body(...)):
    app.detector.stream['config'][param]['value'] = body['value']


@app.put('/stream/api/{version}/command/initialize')
async def stream_initialize(version: Version):
    await app.detector.stream_initialize()


# SYSTEM MODULE ===============================================================

@app.get('/system/api/{version}/status/{param}')
def system_status(version: Version, param: str):
    return app.detector.system['status'][param]


@app.put('/system/api/{version}/command/restart')
async def system_restart(version: Version):
    return await app.detector.system_restart()
