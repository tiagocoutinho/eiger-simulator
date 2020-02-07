import datetime


def utc():
    return datetime.datetime.utcnow()


def utc_str():
    return utc().strftime("%Y-%m-%dT%H:%M:%S.%f")
