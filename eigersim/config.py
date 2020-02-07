import copy
import functools

from .tool import utc_str


class Value(dict):

    def __init__(self, value, value_type='string', **kwargs):
        super().__init__()
        kwargs['value'] = value
        kwargs['value_type'] = value_type
        self.update(kwargs)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        return val() if callable(val) else val


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


def LInt(value, access_mode='rw', **kwargs):
    return Value(value, value_type='int[]', access_mode=access_mode, **kwargs)


def LIntR(value, **kwargs):
    return LInt(value, access_mode='r', **kwargs)


GEN_DETECTOR_CONFIG = {
    'auto_summation': Bool(True),
    'beam_center_x': Float(1533.81, 0.0, 1e6),
    'beam_center_y': Float(1657.1, 0.0, 1e6),
    'bit_depth_image': IntR(16),
    'bit_depth_readout': IntR(12),
    'chi_increment': Float(0.0, -100, 100),
    'chi_start': Float(0.0, -180, 180),
    'compression': Str('bslz4', allowed_values=['lz4', 'bslz4']),
    'count_time': Float(0.5, 0.0000029, 1800, 's'),
    'countrate_correction_applied': Bool(True),
    'countrate_correction_count_cutoff': IntR(12440),
    'data_collection_date': Str(''),
    'description': StrR('Dectris Eiger 9M'),
    'detector_number': StrR('E-18-0102'),
    'detector_distance': Float(0.12696, min=0.0, max=1e6),
    'detector_readout_time': FloatR(1e-5, min=0, max=1e6),
    'element': Str('Cu'),
    'flatfield': Value([], 'float[][]'),
    'flatfield_correction_applied': Bool(True),
    'frame_time': Float(1.0, min=1/500, max=1e6, unit='s'),
    'kappa_increment': Float(0.0, -100, 100),
    'kappa_start': Float(0.0, -180, 180),
    'nimages': Int(10, min=1, max=1_000_000),
    'ntrigger': Int(1, min=1, max=1_000_000),
    'number_of_excluded_pixels': IntR(0),
    'omega_increment': Float(0.0, -100, 100),
    'omega_start': Float(0.0, -180, 180),
    'phi_increment': Float(0.0, -100, 100),
    'phi_start': Float(0.0, -180, 180),
    'photon_energy': Float(12649.9, min=0, max=1_000_000, unit='eV'),
    'pixel_mask': Value([], 'unit[][]'),
    'pixel_mask_applied': Bool(True),
    'roi_mode': Str(''),
    'sensor_material': StrR('Si'),
    'sensor_thickness': FloatR(0.00045),
    'software_version': StrR('1.6.0'),
    'threshold_energy': Float(6324.95),
    'trigger_mode': Str('ints', allowed_values=['ints', 'inte', 'exts', 'exte']),
    'two_theta_increment': Float(0.0, -100, 100),
    'two_theta_start': Float(0.0, -180, 180),
    'wavelength': Float(1.0),
    'x_pixel_size': FloatR(7.5e-5),
    'y_pixel_size': FloatR(7.5e-5),
    'x_pixels_in_detector': IntR(3110),
    'y_pixels_in_detector': IntR(3269),
}


GEN_DETECTOR_STATUS = {
    'state': Value('na', value_type='string', time=utc_str(), state='normal'),
    'error': Value([], value_type='string[]', time=utc_str(), state='normal'),
    'time': Value(utc_str, value_type='date', time=utc_str(), state='normal'),
    'board_000/th0_temp': Value(22.1, value_type='float', time=utc_str(), state='normal'),
    'board_000/th0_humidity': Value(7.45, value_type='float', time=utc_str(), state='normal'),
    'builder/dcu_buffer_free': Value(98.8, value_type='float', time=utc_str(), state='normal'),
}


GEN_MONITOR_CONFIG = {
    'mode': Bool(False),
    'buffer_size': Int(10)
}


GEN_MONITOR_STATUS = {
    'state': StrR('normal'),
    'error': LStrR([]),
    'buffer_fill_level': LIntR([0, 10]),
    'dropped': IntR(0),
    'next_image_number': IntR(0),
    'monitor_image_number': IntR(0),
}


GEN_STREAM_CONFIG = {
    'mode': Str('enabled', allowed_values=['enabled', 'disabled']),
    'header_detail': Str('all', allowed_values=['all', 'basic', 'none']),
    'header_appendix': Str(''),
    'image_appendix': Str(''),
}


GEN_STREAM_STATUS = {
    'state': StrR('ready'),
    'error': LStrR([]),
    'dropped': IntR(0),
}


GEN_SYSTEM_STATUS = {
}


GEN_FILEWRITER_CONFIG = {
    'mode': Str('disabled', allowed_values=['enabled', 'disabled']),
    'transfer_mode': Str('HTTP', allowed_values=['HTTP']),
    'nimages_per_file': Int(1, min=0, max=1_000_000), # 0 means all in master HDF5
    'image_nr_start' : Int(1),
    'name_pattern': Str('series_$id'),
    'compression_enabled': Bool(True),
}


def _copy(base, *args, **kwargs):
    cfg = copy.deepcopy(base)
    for arg in args:
        cfg.update(arg)
    cfg.update(kwargs)
    return cfg


detector_config = functools.partial(_copy, GEN_DETECTOR_CONFIG)
detector_status = functools.partial(_copy, GEN_DETECTOR_STATUS)
monitor_config = functools.partial(_copy, GEN_MONITOR_CONFIG)
monitor_status = functools.partial(_copy, GEN_MONITOR_STATUS)
stream_config = functools.partial(_copy, GEN_STREAM_CONFIG)
stream_status = functools.partial(_copy, GEN_STREAM_STATUS)
system_status = functools.partial(_copy, GEN_SYSTEM_STATUS)
filewriter_config = functools.partial(_copy, GEN_FILEWRITER_CONFIG)
