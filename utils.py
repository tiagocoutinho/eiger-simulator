import h5py
import bitshuffle.h5
# see: https://github.com/kiyo-masui/bitshuffle


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
