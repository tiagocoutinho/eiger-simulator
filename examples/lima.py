import os
import glob
import time
import pathlib
import threading
import urllib.parse

import pint
import numpy
from prompt_toolkit import print_formatted_text, HTML
from prompt_toolkit.shortcuts import ProgressBar

import Lima.Core
import Lima.Eiger
from Lima.Core import CtControl, CtSaving, AcqReady, AcqRunning, Size, FrameDim, Bpp8, Bpp16

ur = pint.UnitRegistry()


class ReportTask:
    DONE = '[<green>DONE</green>]'
    FAIL = '[<red>FAIL</red>]'
    STOP = '[<magenta><b>STOP</b></magenta>]'
    SKIP = '[<yellow>SKIP</yellow>]'

    def __init__(self, message, **kwargs):
        length = kwargs.pop('length', 40)
        template = '{{:.<{}}} '.format(length)
        self.message = template.format(message)
        kwargs.setdefault('end', '')
        kwargs.setdefault('flush', True)
        self.kwargs = kwargs
        self.skipped = False

    def __enter__(self):
        print_formatted_text(HTML(self.message), **self.kwargs)
        self.start = time.time()
        return self

    def skip(self):
        self.DONE = self.SKIP
        self.skipped = True

    @property
    def elapsed(self):
        return self.end - self.start

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.end = time.time()
        if exc_type is None:
            msg = self.DONE
            if not self.skipped:
                elapsed = ((self.elapsed) * ur.s).to_compact()
                msg += ' (took {:~.4})'.format(elapsed)
        elif exc_type is KeyboardInterrupt:
            return False
        else:
            msg = self.FAIL + f'({exc_value!r})'
        print_formatted_text(HTML(msg))
        return False


class AcquisitionContext(Lima.Core.CtControl.ImageStatusCallback):

    def __init__(self, ctrl, cb=None):
        super().__init__()
        self.ctrl = ctrl
        self.cb = cb

    def __enter__(self):
        self.finished = threading.Event()
        self.ctrl.registerImageStatusCallback(self)
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type == KeyboardInterrupt:
            self.stopAcq()
        elif self.status.AcquisitionStatus == AcqRunning:
            self.stopAcq()
        self.ctrl.unregisterImageStatusCallback(self)

    def prepareAcq(self):
        self.ctrl.prepareAcq()

    def startAcq(self):
        self.ctrl.startAcq()

    def stopAcq(self):
        self.ctrl.stopAcq()

    def imageStatusChanged(self, image_status):
        status = self.status
        if self.cb:
            self.cb(status)
        if status.AcquisitionStatus != AcqRunning:
            self.finished.set()

    @property
    def status(self):
        return self.ctrl.getStatus()

    def wait(self):
        return self.finished.wait()


def control(options):
    url = urllib.parse.urlparse(options.url)
    path = url.netloc if url.netloc else url.path
    eiger = Lima.Eiger.Camera(path)
    #eiger.initialize()
    iface = Lima.Eiger.Interface(eiger)
    ctrl = CtControl(iface)
    image = ctrl.image()
    image.setImageType(options.frame_type)
    return ctrl


def configure(ctrl, options):
    th_mgr = Lima.Core.Processlib.PoolThreadMgr.get()
    th_mgr.setNumberOfThread(options.nb_processing_tasks)

    acq = ctrl.acquisition()
    saving = ctrl.saving()
    buff = ctrl.buffer()
    saving.setFormat(options.saving_format)
    saving.setPrefix(options.saving_prefix)
    saving.setSuffix(options.saving_suffix)
    saving.setMaxConcurrentWritingTask(options.nb_saving_tasks)
    if options.saving_directory:
        saving.setSavingMode(saving.AutoFrame)
        saving.setDirectory(options.saving_directory)
    acq.setAcqExpoTime(options.exposure_time)
    acq.setAcqNbFrames(options.nb_frames)
    buff.setMaxMemory(options.max_buffer_size)


def cleanup(ctrl, options):
    pattern = options.saving_prefix + '*' + options.saving_suffix
    pattern = pathlib.Path(options.saving_directory) / pattern
    for filename in glob.glob(str(pattern)):
        os.remove(filename)


def AcqProgBar():
    return ProgressBar(
        title='Acquiring....',
        bottom_toolbar=HTML(" <b>[Control-C]</b> abort")
    )


class AcquisitionMonitor:
    def __init__(self, ctx, prog_bar, options):
        self.ctx = ctx
        nb_frames = options.nb_frames
        self.acq_counter = prog_bar(label='Acquired', total=nb_frames)
        self.base_counter = prog_bar(label='Base Ready', total=nb_frames)
        self.img_counter = prog_bar(label='Ready', total=nb_frames)
        if options.saving_directory:
            self.save_counter = prog_bar(label='Saved', total=nb_frames)
        else:
            self.save_counter = None

    def update(self, status):
        counters = status.ImageCounters
        self.acq_counter.set_items_completed(counters.LastImageAcquired + 1)
        self.base_counter.set_items_completed(counters.LastBaseImageReady + 1)
        self.img_counter.set_items_completed(counters.LastImageReady + 1)
        if self.save_counter:
            self.save_counter.set_items_completed(counters.LastImageSaved + 1)

    def run(self):
        while not self.ctx.finished.is_set():
            self.update(self.ctx.status)
            time.sleep(0.5)
        self.update(self.ctx.status)


def run(options):
    with ReportTask('Initializing'):
        ctrl = control(options)
    with ReportTask('Configuring'):
        configure(ctrl, options)
    with AcquisitionContext(ctrl) as ctx:
        with ReportTask('Preparing'):
            ctx.prepareAcq()
        with ReportTask('Acquiring', end='\n'):
            ctx.startAcq()
            with AcqProgBar() as prog_bar:
                monitor = AcquisitionMonitor(ctx, prog_bar, options)
                monitor.run()
    with ReportTask('Cleaning up') as task:
        if options.no_cleanup or not options.saving_directory:
            task.skip()
        else:
            cleanup(ctrl, options)


def main(args=None):
    import argparse

    def frame_type(text):
        return getattr(Lima.Core, text.capitalize())
    def get_options(namespace, enum):
        return { name: getattr(namespace, name) for name in dir(namespace)
                 if isinstance(getattr(namespace, name), enum) }
    def mem_size(text):
        v = ur(text)
        return v*ur.byte if isinstance(v, (int, float)) else v

    file_format_options = get_options(CtSaving, CtSaving.FileFormat)
    file_format_suffix = {f: '.{}'.format(f.replace('HDF5', 'h5').replace('Format', '').lower())
                          for f in file_format_options}
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('-n', '--nb-frames', default=10, type=int)
    parser.add_argument('-e', '--exposure-time',default=0.1, type=float)
    parser.add_argument('-l', '--latency-time', default=0.0, type=float)
    parser.add_argument('-d', '--saving-directory', default=None, type=str)
    parser.add_argument('-f', '--saving-format', default='EDF', type=str,
                        choices=sorted(file_format_options),
                        help='saving format')
    parser.add_argument('-p', '--saving-prefix', default='image_', type=str)
    parser.add_argument('-s', '--saving-suffix', default='__AUTO_SUFFIX__',
                        type=str)
    parser.add_argument('--frame-type', type=frame_type, default=Bpp16,
                        help='pixel format (ex: Bpp8) [default: Bpp16]')
    parser.add_argument('--max-buffer-size', type=float, default=50,
                        help='maximum buffer size (%% total memory) [default: %(default)s %%]')
    parser.add_argument('--nb-saving-tasks', type=int, default=1,
                        help='nb. of saving tasks [default: %(default)s]')
    parser.add_argument('--nb-processing-tasks', type=int, default=2,
                        help='nb. of processing tasks [default: %(default)s]')
    parser.add_argument('--no-cleanup', default=False, action='store_true',
                        help='do not cleanup saving directory')
    options = parser.parse_args(args)

    options.saving_format_name = options.saving_format
    options.saving_format = file_format_options[options.saving_format]
    if options.saving_suffix == '__AUTO_SUFFIX__':
        options.saving_suffix = file_format_suffix[options.saving_format_name]
    run(options)

if __name__ == '__main__':
    main()

