import enum
import json
import time
import asyncio
import logging
import datetime
import functools

from typing import List

import fastapi
import zmq.asyncio

from . import config
from .dataset import frames_iter
from .tool import utc, utc_str


log = logging.getLogger('eigersim.web')


class Version(str, enum.Enum):
    v1_6_0 = '1.6.0'


class Queue(asyncio.Queue):

    def flush(self):
        flushed = 0
        while not self.empty():
            self.get_nowait()
            self.task_done()
            flushed += 1
        return flushed

    async def __aiter__(self):
        while True:
            yield await self.get()


class ZMQChannel:

    def __init__(self, address='tcp://0:9999', context=None, timeout=0.1,
                 queue_maxsize=10000):
        self.address = address
        self.context = context or zmq.asyncio.Context()
        self.sock = self.context.socket(zmq.PUSH)
        if timeout in (None, 0):
            timeout = 0
        else:
            timeout = -1 if timeout < 0 else int(timeout * 1000)
        self.sock.sndtimeo = timeout
        self.sock.bind(self.address)
        self.queue = Queue(queue_maxsize)
        self.task = None

    async def loop(self):
        queue = self.queue
        sock = self.sock
        async for parts in queue:
            log.info(f'send {len(parts)}')
            try:
                if len(parts) > 1:
                    await sock.send_multipart(parts)
                else:
                    await sock.send(parts[0])
            except zmq.ZMQError as err:
                flushed = queue.flush()
                log.info(f'Error send ZMQ: {err!r}. Flushed {flushed} messages')
            queue.task_done()

    def initialize(self):
        self.task = asyncio.create_task(self.loop())
        self.task.add_done_callback(self.on_task_finished)

    def on_task_finished(self, task):
        print(task.result())

    async def send(self, *parts):
        await self.queue.put(parts)


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
        nb_frames = self.config['nimages']['value']
        log.info(f'start acquisition {nb_frames}')
        frame_time = self.config['frame_time']['value']
        frames = self.frames
        p1_base = dict(htype='dimage-1.0', series=self.series)
        p2_base = dict(htype='dimaged-1.0', shape=frames[0][0].shape,
                              type='uint16') # TODO: ensure data type is correct
        p4_base = dict(htype='dconfig-1.0')
        start = time.time()
        for frame_nb in range(nb_frames):
            log.info(f'[START] frame {frame_nb}')
            frame, encoding = frames[frame_nb]
            p2 = dict(p2_base, size=frame.size, encoding=encoding)
            p1 = dict(p1_base, frame=frame_nb, hash='')
            p3 = frame.data
            parts = [json.dumps(p1).encode(), json.dumps(p2).encode(), p3]
            now = time.time()
            next_time = start + (frame_nb + 1) * frame_time
            sleep_time = next_time - now
            start_time = now - start
            p4 = dict(p4_base, start_time=int(start_time*1e9))
            log.info(f'sleep for {sleep_time}s')
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                log.error(f'overrun at frame {frame_nb}!')
            stop_time = time.time() - start
            p4['stop_time'] = int(stop_time*1e9)
            p4['real_time'] = int((stop_time - start_time)*1e9)
            parts.append(json.dumps(p4).encode())
            await self.zmq.send(*parts)
            log.info(f'[ END ] frame {frame_nb}')

    def _build_dataset(self):
        if self.dataset is None:
            raise NotImplementedError
        else:
            total_size = 0
            frames = []
            for frame in frames_iter(self.dataset):
                total_size += frame.nbytes
                frame = frame, f'bs{frame.dtype.itemsize * 8}-lz4<'
                frames.append(frame)
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

    async def trigger(self, count_time=None):
        if self.acquisition:
            raise RuntimeError('Acquisition already in progress')
        self.acquisition = asyncio.create_task(self.acquire(count_time))
        self.acquisition.add_done_callback(self._on_acquisition_finished)

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

    async def send_global_header_data(self, series):
        detail = self.stream['config']['header_detail']['value']
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
        await self.zmq.send(*parts)

    async def send_end_of_series(self, series):
        header = dict(htype='dseries_end-1.0', series=series)
        await self.zmq.send(json.dumps(header).encode())

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
    return await app.detector.trigger(count_time)


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
