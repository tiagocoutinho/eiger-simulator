# -*- coding: utf-8 -*-
#
# This file is part of the faxtor project
#
# Copyright (c) 2019 Tiago Coutinho
# Distributed under the MIT. See LICENSE for more info.

import os
import glob
import time
import queue
import pathlib
import threading
import contextlib
import urllib.parse

import pint
import tqdm
import click
import numpy

import Lima.Core
import Lima.Eiger
from Lima.Core import CtControl, CtSaving, AcqReady, AcqRunning, Size, FrameDim, Bpp8, Bpp16

ur = pint.UnitRegistry()


class ReportTask:
    DONE = '[{}]'.format(click.style('DONE', fg='green'))
    FAIL = '[{}]'.format(click.style('FAIL', fg='red'))
    STOP = '[{}]'.format(click.style('STOP', fg='magenta'))
    SKIP = '[{}]'.format(click.style('SKIP', fg='yellow'))

    def __init__(self, message, **kwargs):
        length = kwargs.pop('length', 40)
        template = '{{:.<{}}} '.format(length)
        self.message = template.format(message)
        kwargs.setdefault('nl', False)
        self.kwargs = kwargs
        self.skipped = False

    def __enter__(self):
        click.secho(self.message, **self.kwargs)
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
        elif exc_type == KeyboardInterrupt:
            msg = self.STOP
        else:
            msg = self.FAIL + f'({exc_value!r})'
        click.secho(msg)
        return False


class ProgressBar(tqdm.tqdm):
    def __init__(self, *args, **kwargs):
        self.event = kwargs.pop('event')
        super().__init__(*args, **kwargs)

    def update_status(self, status):
        n = getattr(status.ImageCounters, self.event) + 1
        progress = n - self.n
        if progress > 0:
            self.update(progress)


class AcquisitionContext(CtControl.ImageStatusCallback):

    def __init__(self, ctrl, cb=None):
        self.ctrl = ctrl
        self.cb = cb or (lambda status: None)
        self.finished = threading.Event()
        super().__init__()

    def __enter__(self):
        self.ctrl.registerImageStatusCallback(self)
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type == KeyboardInterrupt:
            self.ctrl.stopAcq()
        self.finished.wait()
        self.ctrl.unregisterImageStatusCallback(self)
        del self.ctrl

    def imageStatusChanged(self, image_status):
        status = self.ctrl.getStatus()
        if status.AcquisitionStatus != AcqRunning:
            self.finished.set()
        self.cb(status)


def print_information(options):
    fsize = Size(3110, 3269)
    frame_dim = FrameDim(fsize, options.frame_type)
    exposure_time = options.exposure_time * ur.second
    acq_time = (options.nb_frames * exposure_time).to_compact()
    frame_rate = (1/exposure_time).to(ur.Hz).to_compact()
    print('Information -------------------------------------------------------')
    print('Frame size: {}'.format(frame_dim))
    print('Acquisition speed: {:~.4}'.format(frame_rate))
    print('Total acquisition time: {:~.4}'.format(acq_time))
    print('-------------------------------------------------------------------')
    print()


def print_stats(options, acq_task):
    elapsed = ((acq_task.elapsed) * ur.s).to_compact()
    print('Stats -------------------------------------------------------------')
    print('Total time: {:~.4}'.format(elapsed))
    print('-------------------------------------------------------------------')
    print()


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
    saving.setMaxConcurrentWritingTask(options.saving_tasks)
    if options.saving_directory:
        saving.setSavingMode(saving.AutoFrame)
        saving.setDirectory(options.saving_directory)
    acq.setAcqExpoTime(options.exposure_time)
    acq.setAcqNbFrames(options.nb_frames)
    buff.setMaxMemory(options.max_buffer_size)


def prepare(ctrl, options):
    ctrl.prepareAcq()


def acquisition_generator(ctrl, events, cb, options):
    ctrl.startAcq()
    while not cb.finished.is_set():
        yield events.get()


def acquisition_wait(ctrl, events, cb, options):
    ctrl.startAcq()
    cb.finished.wait()


def acquisition_progress(ctrl, events, cb, options):
    nb_frames = options.nb_frames
    total_acq_time = nb_frames * options.exposure_time
    acq_pb = ProgressBar(total=nb_frames, position=0, event='LastImageReady',
                         desc='  Ready', unit='frame')
    bars = [acq_pb]
    if options.saving_directory:
        sav_pb = ProgressBar(total=nb_frames, position=1, event='LastImageSaved',
                             desc='  Saved', unit='frame')
        bars.append(sav_pb)
    else:
        sav_pb = contextlib.nullcontext()
    acq_gen = acquisition_generator(ctrl, events, cb, options)
    with acq_pb, sav_pb:
        for status in acq_gen:
            for bar in bars:
                bar.update_status(status)
    print()


def stop(ctrl, options, event=None):
    ctrl.stopAcq()
    if event:
        event.wait()


def cleanup(ctrl, options):
    pattern = options.saving_prefix + '*' + options.saving_suffix
    pattern = pathlib.Path(options.saving_directory) / pattern
    for filename in glob.glob(str(pattern)):
        os.remove(filename)


def run(options):
    print_information(options)

    if options.dry_run:
        return

    total_acq_time = options.nb_frames * options.exposure_time
    progress = not options.no_progressbar
    ack_task = None
    try:
        with ReportTask('Initializing'):
            ctrl = control(options)
        with ReportTask('Configuring'):
            configure(ctrl, options)
        events = queue.Queue()
        with AcquisitionContext(ctrl, events.put) as cb:
            with ReportTask('Preparing'):
                prepare(ctrl, options)
            if progress:
                with ReportTask('Acquiring', nl=True) as acq_task:
                    acquisition_progress(ctrl, events, cb, options)
            else:
                with ReportTask('Acquiring') as acq_task:
                    acquisition_wait(ctrl, events, cb, options)
    except KeyboardInterrupt:
        click.secho('Ctrl-C pressed. Bailing out!', fg='yellow')
    finally:
        with ReportTask('Cleaning up') as task:
            if options.no_cleanup or not options.saving_directory:
                task.skip()
            else:
                cleanup(ctrl, options)
    if ack_task:
        print_stats(options, acq_task)


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
    parser.add_argument('--saving-tasks', type=int, default=1,
                        help='nb. of saving tasks [default: %(default)s]')
    parser.add_argument('--nb-processing-tasks', type=int, default=2,
                        help='nb. of processing tasks [default: %(default)s]')
    parser.add_argument('--dry-run', default=False, action='store_true',
                        help='if set only print information and exit')
    parser.add_argument('--no-cleanup', default=False, action='store_true',
                        help='do not cleanup saving directory')
    parser.add_argument('--no-progressbar', default=False, action='store_true',
                        help='do not show progress bar')
    options = parser.parse_args(args)

    options.saving_format_name = options.saving_format
    options.saving_format = file_format_options[options.saving_format]
    if options.saving_suffix == '__AUTO_SUFFIX__':
        options.saving_suffix = file_format_suffix[options.saving_format_name]
    run(options)


if __name__ == '__main__':
    main()

