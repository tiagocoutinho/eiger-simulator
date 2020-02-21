import time
import logging
import argparse
import threading
import urllib.parse

import Lima.Eiger
import Lima.Core

log = logging.getLogger('lima.eiger.demo')

StatusMap = {
    Lima.Core.AcqReady: 'Ready',
    Lima.Core.AcqRunning: 'Running',
    Lima.Core.AcqFault: 'Fault'
}


class CB(Lima.Core.CtControl.ImageStatusCallback):

    def __init__(self, ctrl, finished_event):
        super().__init__()
        self.ctrl = ctrl
        self.finished_event = finished_event
        self.nb_frames = None
        self.first_frame_acquired_ts = None
        self.last_frame_acquired_ts = None
        self.last_status = None

    def imageStatusChanged(self, image_status):
        ts = time.time()
        status = self.ctrl.getStatus()
        self.last_status = status
        acq_nb = status.ImageCounters.LastImageAcquired
        if self.first_frame_acquired_ts is None and acq_nb == 0:
            self.first_frame_acquired_ts = ts
        if acq_nb == (self.nb_frames - 1):
            self.last_frame_acquired_ts = ts
        msg = f'{StatusMap[status.AcquisitionStatus]:7} ' \
              f'#IAcq={status.ImageCounters.LastImageAcquired:04} ' \
              f'#IBase={status.ImageCounters.LastBaseImageReady:04} ' \
              f'#IReady={status.ImageCounters.LastImageReady:04} ' \
              f'#ISaved={status.ImageCounters.LastImageSaved:04} ' \
              f'#Counter={status.ImageCounters.LastCounterReady:04} '
        log.info(msg)
        if status.AcquisitionStatus != Lima.Core.AcqRunning:
            self.finished_event.set()

    def clear(self):
        return self.finished_event.clear()

    def wait(self):
        return self.finished_event.wait()


def new_lima_eiger(url):
    log.info('[START] create lima')
    eiger = Lima.Eiger.Camera(url)
    #eiger.initialize()
    iface = Lima.Eiger.Interface(eiger)
    ctrl = Lima.Core.CtControl(iface)
    ctrl.camera = eiger
    ctrl.interface = iface
    finished_event = threading.Event()
    cb = CB(ctrl, finished_event)
    ctrl.callback = cb
    ctrl.registerImageStatusCallback(cb)
    log.info('[ END ] create lima')
    return ctrl


def init_lima_eiger(ctrl, options):
    log.info('[START] init lima')
    img = ctrl.image()
    img.setImageType(Lima.Core.Bpp16)
    buff = ctrl.buffer()
    buff.setMaxMemory(options.max_buffer_size)

    log.info('[ END ] init lima')


def clean_lima_eiger(ctrl):
    ctrl.unregisterImageStatusCallback(ctrl.callback)
    del ctrl.interface
    del ctrl.camera
    del ctrl.callback


def prepare(ctrl, expo_time, nb_frames):
    log.info('[START] prepare lima')
    acq = ctrl.acquisition()
    acq.setAcqExpoTime(expo_time)
    acq.setAcqNbFrames(nb_frames)
    ctrl.callback.nb_frames = nb_frames
    ctrl.prepareAcq()
    log.info('[ END ] prepare lima')


def acquire(ctrl):
    log.info('[ START ] acquisition')
    cb = ctrl.callback
    start_time = time.time()
    ctrl.startAcq()
    try:
        cb.wait()
    finally:
        end_time = time.time()
        acq_status = cb.last_status.AcquisitionStatus
        if acq_status == Lima.Core.AcqReady:
            acq = ctrl.acquisition()
            first, last = cb.first_frame_acquired_ts, cb.last_frame_acquired_ts
            log.info('acq finished normally. Frames received dt = %fs (ideal = %fs)',
                     last - first, acq.getAcqExpoTime()*acq.getAcqNbFrames())
        elif acq_status == Lima.Core.AcqFault:
            ctrl.stopAcq()
            log.error('acq finished in FAULT')
        else:
            log.warning('acq finished with unexpected status %r', StatusMap[acq_status])
        log.info('[ END ] acquisition (took %fs)', end_time - start_time)
        cb.clear()


def run(options):
    url = urllib.parse.urlparse(options.url)
    path = url.netloc if url.netloc else url.path

    th_mgr = Lima.Core.Processlib.PoolThreadMgr.get()
    th_mgr.setNumberOfThread(options.nb_processing_tasks)

    ctrl = new_lima_eiger(path)
    init_lima_eiger(ctrl, options)
    prepare(ctrl, options.exposure_time, options.nb_frames)
    try:
        acquire(ctrl)
    except:
        log.exception('acquisition finished in error:')
    finally:
        clean_lima_eiger(ctrl)
    log.info('Finished!')


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('-n', '--nb-frames', default=1, type=int)
    parser.add_argument('-t', '--exposure-time', default=1.0, type=float)
    parser.add_argument('--log-level', help='log level', type=str,
                        default='INFO',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])
    parser.add_argument('--nb-processing-tasks', type=int, default=2,
                        help='nb. of processing tasks [default: %(default)s]')
    parser.add_argument('--max-buffer-size', type=float, default=50,
                        help='maximum buffer size (%% total RAM) [default: %(default)s%%]')
    options = parser.parse_args(args)
    log_level = getattr(logging, options.log_level.upper())
    log_fmt = '%(levelname)s %(asctime)-15s %(name)s: %(message)s'
    logging.basicConfig(level=log_level, format=log_fmt)
    #Lima.Core.DebParams.setTypeFlagsNameList(['Fatal', 'Error', 'Warning', 'Trace', 'Param', 'Return'])
    run(options)


if __name__ == '__main__':
    main()
