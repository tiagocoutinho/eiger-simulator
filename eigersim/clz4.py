import ctypes.util

import numpy

c_int = ctypes.c_int
c_char_p = ctypes.POINTER(ctypes.c_char)


def _load_lib(name='lz4'):
    lz4_name = ctypes.util.find_library(name)
    if not name:
        raise ValueError(f'Cannot find {name}')
    lib = ctypes.CDLL(lz4_name)
    lib.LZ4_compress_default.argtypes = [c_char_p, c_char_p, c_int, c_int]
    lib.LZ4_compress_default.restype = c_int
    lib.LZ4_decompress_safe.argtypes = [c_char_p, c_char_p, c_int, c_int]
    lib.LZ4_decompress_safe.restype = c_int
    return lib


lz4 = _load_lib()

def compress(src_arr):
    src_data = src_arr.ctypes.data_as(c_char_p)
    src_size = src_arr.nbytes
    dst_data = ctypes.create_string_buffer(src_size*2)
    dst_size = len(dst_data)
    n = lz4.LZ4_compress_default(src_data, dst_data, src_size, dst_size)
    if not n:
        raise ValueError('Failed to compress ({})'.format(n))
    return dst_data[:n]


def decompress(buff, shape, dtype='u2'):
    src_size = len(buff)
    arr_type = ctypes.c_char * src_size
    src_data = ctypes.cast(buff, ctypes.c_char_p)
    numpy_dtype = numpy.dtype(dtype)
    dst_size = shape[0] * shape[1] * numpy_dtype.itemsize
    dst_data = ctypes.create_string_buffer(dst_size)
    n = lz4.LZ4_decompress_safe(src_data, dst_data, src_size, dst_size)
    if n <= 0:
        raise ValueError('Failed to decompress ({})'.format(n))
    result = numpy.frombuffer(dst_data, dtype=numpy_dtype)
    result.shape = shape
    return result
