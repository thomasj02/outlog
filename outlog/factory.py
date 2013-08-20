import time
import ujson
import zmq

__author__ = 'tjohnson'


def get_microtime():
    """
    Gets the current timestamp in microseconds

    :return: The current timestamp in microseconds
    :rtype: int
    """
    return int(time.time() * 1000. * 1000.)


class MessageFactory(object):
    """
    A message factory that creates and returns Outlog messages
    """

    def __init__(self, hostname, application, microtime_function=get_microtime):
        """
        :param hostname: The name of the host where the log message was generated
        :param application: The name of the application that generated the log message
        :param microtime_function: The function we should call to get the microsecond timestamp
        """
        self.hostname = hostname
        self.application = application
        self.microtime_function = microtime_function

    def msg(self, message_class, level, **kwargs):
        """
        Create a Outlog log message

        :param message_class: The class of the log message to create
        :param level: The logging level (user-defined)
        :param kwargs: Any additional arguments to pass to create the log message
        :return: A subclass of outlog.msgs.BaseMessage
        :rtype: outlog.msgs.BaseMessage
        """
        return message_class(
            hostname=self.hostname, microtime=self.microtime_function(), application=self.application,
            level=level, **kwargs)


class JsonFileMessageFactory(MessageFactory):
    """
    A message factory that creates outlog messages and writes them to a file, json-encoded
    """

    def __init__(self, hostname, application, microtime_function=get_microtime, file_handle=None):
        """
        :param hostname: The name of the host where the log message was generated
        :param application: The name of the application that generated the log message
        :param microtime_function: The function we should call to get the microsecond timestamp
        :param file_handle: A file handle that supports write()
        """

        if not file_handle:
            raise RuntimeError("OutlogJsonFileMessageFactory requires a file handle")

        super(JsonFileMessageFactory, self).__init__(hostname, application, microtime_function)
        self.file_handle = file_handle

    def msg(self, message_class, level, **kwargs):
        """
        Create a Outlog log message and write it to the file

        :param message_class: The class of the log message to create
        :param level: The logging level (user-defined)
        :param kwargs: Any additional arguments to pass to create the log message
        :return: A subclass of outlog.msgs.BaseMessage
        :rtype: outlog.msgs.BaseMessage
        """
        message = super(JsonFileMessageFactory, self).msg(message_class, level, **kwargs)
        self.file_handle.write(ujson.dumps(message))
        self.file_handle.write("\n")
        return message


class ZmqMessageFactory(MessageFactory):
    def __init__(self, hostname, application, microtime_function=get_microtime, socket=None):
        """
        :param hostname: The name of the host where the log message was generated
        :param application: The name of the application that generated the log message
        :param microtime_function: The function we should call to get the microsecond timestamp
        :type socket: zmq.Socket
        """

        if not socket:
            raise RuntimeError("OutlogZmqFileMessageFactory requires a zmq socket")

        super(JsonFileMessageFactory, self).__init__(hostname, application, microtime_function)
        self.socket = socket

    def msg(self, message_class, level, **kwargs):
        """
        Create a Outlog log message and send it over the zmq socket, json-encoded

        :param message_class: The class of the log message to create
        :param level: The logging level (user-defined)
        :param kwargs: Any additional arguments to pass to create the log message
        :return: A subclass of outlog.msgs.BaseMessage
        :rtype: outlog.msgs.BaseMessage
        """

        message = super(JsonFileMessageFactory, self).msg(message_class, level, **kwargs)
        ujson_message = ujson.dumps(message)
        self.socket.send(ujson_message, zmq.NOBLOCK, copy=False)
        return message
