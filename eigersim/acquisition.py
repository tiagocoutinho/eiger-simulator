import json
import time
import logging
import itertools

import zmq

log = logging.getLogger('eigersim.acquisition')


def acquire(count_time, nb_frames, series, dataset, zmq_channel):
    log.info(f'[START] acquisition #{series} (prepare)')
    _, f0, encoding = dataset[0]
    p1_base = dict(htype='dimage-1.0', series=series)
    p2_base = dict(htype='dimaged-1.0', shape=f0.shape, size=f0.size,
                   encoding=encoding, type='uint16') # TODO: ensure data type is correct
    p4_base = dict(htype='dconfig-1.0')

    frames = itertools.cycle(dataset)
    p2s = (dict(p2_base) for i in range(nb_frames))
    p2s = [zmq.Frame(json.dumps(p).encode()) for p in p2s]
    p1s = (dict(p1_base, frame=i, hash='') for i in range(nb_frames))
    p1s = [zmq.Frame(json.dumps(p).encode()) for p in p1s]
    p3s = [frame for i, (frame, _, _) in zip(range(nb_frames), frames)]
    p4s = []
    log.info(f'[START] acquisition #{series} (start)')
    start = time.time()
    for frame_nb in range(nb_frames):
        start_nano = int((start + frame_nb * count_time) * 1e9)
        stop_nano = int((start + (frame_nb + 1) * count_time) * 1e9)
        real_nano = stop_nano - start_nano
        p4 = dict(p4_base, start_time=start_nano, stop_time=stop_nano,
                  real_time=real_nano)
        p4 = zmq.Frame(json.dumps(p4).encode())
        p4s.append(p4)
    parts = [(p1, p2, p3, p4) for p1, p2, p3, p4 in zip(p1s, p2s, p3s, p4s)]
    start = time.monotonic()
    for frame_nb in range(nb_frames):
        log.debug(f'  [START] frame {frame_nb}')
        frame_parts = parts[frame_nb]
        now = time.monotonic()
        next_time = start + (frame_nb + 1) * count_time
        sleep_time = next_time - now
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            log.error(f'overrun at frame {frame_nb}!')
        if zmq_channel:
            zmq_channel.send(*frame_parts)
        log.debug(f'  [ END ] frame {frame_nb}')
    log.info(f'[ END ] acquisition')


