import os
import pickle
import logging
import pathlib
import concurrent.futures

import h5py
import bitshuffle.h5

import numpy

from . import clz4

log = logging.getLogger('eigersim.dataset')


class DataHDF5:
    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode
        self.file = None
        self.data = None

    def __enter__(self):
        print(self.filename)
        self.file = h5py.File(self.filename, 'r')
        self.data = self.file['entry']['data']
        return self.data

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


# MODE 1: frames from dataset


def frames_iter_dataset(filename, compression='lz4'):
    """Iterate over frames from a dataset stored in disk"""
    filename = pathlib.Path(filename)
    if filename.is_dir():
        cache_directory = filename
    elif filename.suffix == '.h5':
        cache_directory = filename.parent / '__cache__'
        if not cache_directory.is_dir():
            _build_dataset_cache(filename, cache_directory)
    for fname in cache_directory.iterdir():
        if not fname.name.endswith('.{}.pickle'.format(compression)):
            continue
        with open(fname, 'rb') as fobj:
            dataset = pickle.load(fobj)
        for frame in dataset:
            yield frame


def _build_dataset_cache(filename, dest=None):
    filename = pathlib.Path(filename)
    if dest is None:
        dest = filename.parent / '__cache__'
    dest = pathlib.Path(dest)
    dest.mkdir(exist_ok=True)
    nb_cpus = os.cpu_count()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=nb_cpus)
    tasks = []
    for name, dataset in _h5_iter(filename):
        task = executor.submit(_save_dataset_lz4, name, dest, dataset)
        tasks.append(task)
        task = executor.submit(_save_dataset_bslz4, name, dest, dataset)
        tasks.append(task)
    concurrent.futures.wait(tasks)


def _save_dataset_lz4(name, dest_dir, dataset):
    filename = dest_dir / name
    filename = filename.with_suffix('.lz4.pickle')
    log.info("[START] LZ4 of %r...", name)
    frames = [clz4.compress(frame) for frame in dataset]
    log.info("[ END ] LZ4 of %r", name)
    log.info("[START] save %r...", filename)
    with open(filename, 'wb') as fobj:
        pickle.dump(frames, fobj)
    log.info("[ END ] save %r", filename)


def _save_dataset_bslz4(name, dest_dir, dataset):
    filename = dest_dir / name
    filename = filename.with_suffix('.bslz4.pickle')
    log.info("[START] BS-LZ4 of %r...", name)
    frames = [bitshuffle.compress_lz4(frame).tobytes() for frame in dataset]
    log.info("[ END ] BS-LZ4 of %r", name)
    log.info("[START] save %r...", filename)
    with open(filename, 'wb') as fobj:
        pickle.dump(frames, fobj)
    log.info("[ END ] save %r", filename)


def _h5_iter(filename):
    buff = None
    with DataHDF5(filename, 'r') as data:
        for name in data:
            try:
                frames = data[name]
            except KeyError:
                log.warning('skipping empty dataset')
                continue
            assert frames.ndim == 3
            buff = numpy.empty(frames.shape, dtype=frames.dtype)
            log.info("[START] loading H5 file %r with %s frames...",
                     name, len(frames))
            frames.read_direct(buff)
            log.info("[ END ] loading H5 file %r", name)
            yield name, buff


# MODE 2: frames generated on the fly

def new_frame(x, y, dtype, frame_nb):
    data = numpy.random.randint(0, 2**16, size=(x, y), dtype=dtype)
    data[100:-100, 100:-100] = frame_nb
    return data


def new_frame(x, y, dtype, frame_nb):
    data = numpy.arange(x*y, dtype=dtype)
    data.shape = x, y
    data[:500, :500] = frame_nb*100
    return data


def frames_iter_gen(filename, x=3110, y=3269, dtype='uint16'):
    """Iterate over frames generated on the fly"""
    i = 0
    while True:
        data = new_frame(x, y, dtype, i)
        yield clz4.compress(data)
        i += 1


frames_iter = frames_iter_dataset
