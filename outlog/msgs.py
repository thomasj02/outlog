__author__ = 'tjohnson'

class BaseMessage(object):
    def __init__(self, hostname, microtime, application, level, message_name):
        self.hostname = hostname
        self.microtime = microtime
        self.application = application
        self.level = level
        self.message_name = message_name

    def __repr__(self):
        return "{microtime} {application}.{hostname} {message_name} {level}".format(**self.__dict__)


class Heartbeat(BaseMessage):
    def __init__(self, hostname, microtime, application, level):
        super(Heartbeat, self).__init__(
            hostname=hostname, microtime=microtime, application=application, level=level, message_name="Heartbeat")
