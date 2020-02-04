from enum import Enum

from fastapi import FastAPI


class Param(str, Enum):
    count_time = 'count_time'
    frame_time = 'frame_time'
    description = 'description'
    detector_instance = 'detector_instance'


app = FastAPI()

detector = {
    'config': {
        Param.count_time: { # Exposure time per image
            "min" : 0.000002999900061695371,
            "max" : 1800,
            "value" : 0.5,
            "value_type" : "float",
            "access_mode" : "rw",
            "unit" : "s"
        }
        Param.description: {
            "value_type": "string",
            "access_mode" : "r",
    },
    'status': {

    }
}

# DETECTOR MODULE =============================================================

# CONFIG task -----------------------------------------------------------------

@app.get('/detector/api/{version}/config/{param}')
def config(version: str, param: Param):
    assert version.startswith('1.6')
    return detector['config'][param]


@app.put('/detector/api/{version}/config/{param}')
def config(version: str, param: Param, value: float):
    assert version.startswith('1.6')
    par = detector['config'][param]
    assert value >= par['min']
    assert value <= par['max']
    par['value'] = value
    return ["bit_depth_image", "count_time",
            "countrate_correction_count_cutoff",
            "frame_count_time", "frame_period", "nframes_sum"]


# STATUS task -----------------------------------------------------------------

@app.get('/detector/api/{version}/status/{param}')
def status(version: str, param: Param):
    assert version.startswith('1.6')
    return detector['status'][param]


# COMMAND task ----------------------------------------------------------------

@app.put('/detector/api/{version}/command/initialize')
def initialize(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/arm')
def arm(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/disarm')
def disarm(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/trigger')
def trigger(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/cancel')
def cancel(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/abort')
def abort(version: str):
    assert version.startswith('1.6')


@app.put('/detector/api/{version}/command/status_update')
def status_update(version: str):
    assert version.startswith('1.6')


# FILE WRITER MODULE ==========================================================

@app.get('/filewriter/api/{version}/{module}')
def filewriter(version: str, module: str):
    assert version.startswith('1.6')


# STREAM MODULE ===============================================================

@app.get('/stream/api/{version}/{module}')
def stream(version: str, module: str):
    assert version.startswith('1.6')


# MONITOR MODULE ==============================================================

@app.get('/monitor/api/{version}/')
def monitor(version: str):
    assert version.startswith('1.6')
    pass


# SYSTEM MODULE ===============================================================

@app.get('/system/api/{version}/')
def system(version: str):
    assert version.startswith('1.6')
    pass


def main():
    import uvicorn
    uvicorn.run("eigersim:app", host="0", port=5000, log_level="info")
