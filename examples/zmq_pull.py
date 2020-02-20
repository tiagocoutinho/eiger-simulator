import json
import zmq
import numpy

from eigersim import clz4

ctx = zmq.Context()

sock = ctx.socket(zmq.PULL)
sock.connect('tcp://127.0.0.1:9999')

def loop(sock):
    while True:
        data = sock.recv_multipart()
        p1 = json.loads(data[0])
        htype = p1['htype']
        if htype.startswith('dheader') or htype.startswith('dseries_end'):
            for part in data:
                print(json.loads(part))
        elif htype.startswith('dimage'):
            dimage, dimage_d, buff, dconfig = data[:4]
            dimage = json.loads(dimage)
            dimage_d = json.loads(dimage_d)
            dconfig = json.loads(dconfig)
            print(dimage)
            print(dimage_d)
            if dimage_d['encoding'].startswith('lz4'):
                #buff = lz4.frame.decompress(buff)
                #arr = numpy.frombuffer(buff, dtype=numpy.uint16)
                arr = clz4.decompress(buff, dimage_d['shape'])
                print(arr)
            else:
                print('data {!r}: {}b'.format(dimage_d['encoding'], len(buff)))
            print(dconfig)
        else:
            print('UNKNOWN', p1)

loop(sock)
