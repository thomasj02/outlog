import time
import ujson

__author__ = 'tjohnson'


def get_microtime():
    return int(time.time()*1000.*1000.)


class OutlogMessageFactory(object):
    def __init__(self, hostname, application, microtime_function=get_microtime):
        self.hostname = hostname
        self.application = application
        self.microtime_function = microtime_function

    def msg(self, message_class, level, **kwargs):
        return message_class(
            hostname=self.hostname, microtime=self.microtime_function(), application=self.application,
            level=level, **kwargs)


class OutlogJsonFileMessageFactory(OutlogMessageFactory):
    def __init__(self, hostname, application, microtime_function=get_microtime, file_handle=None):
        super(OutlogJsonFileMessageFactory, self).__init__(hostname, application, microtime_function)
        self.file_handle = file_handle

    def msg(self, message_class, level, **kwargs):
        message = super(OutlogJsonFileMessageFactory, self).msg(message_class, level, **kwargs)
        self.file_handle.write(ujson.dumps(message))
        self.file_handle.write("\n")
