import os
import logging
import pathlib
import concurrent.futures

import h5py
import numpy
import bitshuffle.h5
# see: https://github.com/kiyo-masui/bitshuffle

log = logging.getLogger('eigersim.dataset')


class DataHDF5:
    def __init__(self, filename, mode):
        self.filename = filename
        self.mode = mode
        self.file = None
        self.data = None

    def __enter__(self):
        self.file = h5py.File(self.filename, 'r')
        self.data = self.file['entry']['data']
        return self.data

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


def frames_iter(filename):
    filename = pathlib.Path(filename)
    if filename.is_dir():
        cache_directory = filename
    elif filename.suffix == '.h5':
        cache_directory = filename.parent / '__cache__'
        if not cache_directory.is_dir():
            _build_dataset_cache(filename, cache_directory)
    for fname in cache_directory.iterdir():
        if fname.suffix != '.npz':
            continue
        dataset = numpy.load(fname)
        for frame in dataset:
            yield dataset[frame]


def _build_dataset_cache(filename, dest=None):
    filename = pathlib.Path(filename)
    if dest is None:
        dest = filename.parent / '__cache__'
    dest = pathlib.Path(dest)
    dest.mkdir(exist_ok=True)
    tp = concurrent
    for name, dataset in _h5_iter(filename):
        _save_dataset(name, dest, dataset)


def _save_dataset(name, dest_dir, dataset):
    npy_filename = dest_dir / name
    log.info("[START] bitshuffle + LZ4 of %r...", name)
    frames = [bitshuffle.compress_lz4(frame) for frame in dataset]
    log.info("[ END ] bitshuffle + LZ4 of %r", name)
    log.info("[START] save %r...", npy_filename)
    numpy.savez(npy_filename, *frames)
    log.info("[ END ] save %r", npy_filename)


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
#            if buff is None or buff.shape != frames.shape or buff.dtype != frames.dtype:
            buff = numpy.empty(frames.shape, dtype=frames.dtype)
            log.info("[START] loading H5 file %r with %s frames...",
                     name, len(frames))
            frames.read_direct(buff)
            log.info("[ END ] loading H5 file %r", name)
            yield name, buff


def _frames_iter(filename):
    buff = None
    with DataHDF5(filename, 'r') as data:
        for name in data:
            frames = data[name]
            assert frames.ndim == 3
            if buff is None or buff.shape != frames.shape or buff.dtype != frames.dtype:
                buff = numpy.empty(frames.shape, dtype=frames.dtype)
            print("loading datafile {1} with {0}"
                  " frames...".format(len(frames), name))
            frames.read_direct(buff)
            for frame in buff:
                yield frame


def get_frames(filename, max_frames=None):

    _frames = list()
    _dtype = set()
    _fshape = set()
    loaded = False

    with DataHDF5(filename, 'r') as data:
        _nframes = 0
        loaded_frames = 0
        while not max_frames or not loaded:
            for name in data.keys():
                if not loaded:
                    try:
                        _nframes = data[name].shape[0]
                        print("loading datafile {1} with {0}"
                              " frames...".format(_nframes, name))
                        for _frame_num in range(_nframes):
                            # returns each frame as numpy.ndarray
                            _new_frame = data[name][_frame_num]
                            _dtype.add(_new_frame.dtype)
                            _fshape.add(_new_frame.shape)
                            _frames.append(data[name][_frame_num])
                            loaded_frames += 1
                            if max_frames and loaded_frames == max_frames:
                                loaded = True
                                break
                    except Exception as e:
                        pass
                else:
                    break

    if len(_dtype) != 1:
        msg = "Not all frames have the same data type\n"
        msg += "dtypes = {0}".format(_dtype)
        raise RuntimeError(msg)

    if len(_fshape) != 1:
        msg = "Not all frames have the same dimensions\n"
        msg += "dimensions = {0}".format(_dtype)
        raise RuntimeError(msg)

    print("{0} frames loaded".format(len(_frames)))
    return _frames


if __name__ == "__main__":
    fn = "/local/EIGER_9M_datasets/transthyretin_200Hz/" \
         "200Hz_0.1dpf_1_master.h5"
    frames = get_frames(fn, 100)
