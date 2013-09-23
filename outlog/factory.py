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

    def __init__(self, hostname, application, microtime_function=get_microtime, postprocessing_functions=[]):
        """
        :param hostname: The name of the host where the log message was generated
        :param application: The name of the application that generated the log message
        :param microtime_function: The function we should call to get the microsecond timestamp
        """
        self.hostname = hostname
        self.application = application
        self.microtime_function = microtime_function
        self.postprocessing_functions = postprocessing_functions

    def _apply_postprocessing(self, message):
        """
        You can use postprocessing functions to apply various postprocessing transformations to the message after it
        gets created but before it gets logged.
        """
        for postprocessing_function in self.postprocessing_functions:
            postprocessing_function(message)

    def msg(self, message_class, level, **kwargs):
        """
        Create a Outlog log message

        :param message_class: The class of the log message to create
        :param level: The logging level (user-defined)
        :param kwargs: Any additional arguments to pass to create the log message
        :return: A subclass of outlog.msgs.BaseMessage
        :rtype: outlog.msgs.BaseMessage
        """
        message = message_class(
            hostname=self.hostname, microtime=self.microtime_function(), application=self.application,
            level=level, **kwargs)
        self._apply_postprocessing(message)
        return message


class SerializedFileMessageFactory(MessageFactory):
    """
    A message factory that creates outlog messages and serializes them to a file
    """

    def __init__(self, hostname, application, microtime_function=get_microtime, file_handle=None, serializer=ujson.dumps):
        """
        :param hostname: The name of the host where the log message was generated
        :param application: The name of the application that generated the log message
        :param microtime_function: The function we should call to get the microsecond timestamp
        :param file_handle: A file handle that supports write()
        :param serializer: The serializer function, which returns the serialized data when given a message
        """

        if not file_handle:
            raise RuntimeError("SerializedFileMessageFactory requires a file handle")

        super(SerializedFileMessageFactory, self).__init__(hostname, application, microtime_function)
        self.file_handle = file_handle
        self.serializer = serializer

    def msg(self, message_class, level, **kwargs):
        """
        Create a Outlog log message and write it to the file

        :param message_class: The class of the log message to create
        :param level: The logging level (user-defined)
        :param kwargs: Any additional arguments to pass to create the log message
        :return: A subclass of outlog.msgs.BaseMessage
        :rtype: outlog.msgs.BaseMessage
        """
        message = super(SerializedFileMessageFactory, self).msg(message_class, level, **kwargs)
        self.file_handle.write(self.serializer(message))
        self.file_handle.write("\n")
        return message


class ZmqMessageFactory(MessageFactory):
    def __init__(self, hostname, application, microtime_function=get_microtime, socket=None, serializer=ujson.dumps):
        """
        :param hostname: The name of the host where the log message was generated
        :param application: The name of the application that generated the log message
        :param microtime_function: The function we should call to get the microsecond timestamp
        :type socket: zmq.Socket
        """

        if not socket:
            raise RuntimeError("ZmqFileMessageFactory requires a zmq socket")

        super(ZmqMessageFactory, self).__init__(hostname, application, microtime_function)
        self.socket = socket
        self.serializer = serializer

    def msg(self, message_class, level, **kwargs):
        """
        Create a Outlog log message and send it over the zmq socket, json-encoded

        :param message_class: The class of the log message to create
        :param level: The logging level (user-defined)
        :param kwargs: Any additional arguments to pass to create the log message
        :return: A subclass of outlog.msgs.BaseMessage
        :rtype: outlog.msgs.BaseMessage
        """

        message = super(ZmqMessageFactory, self).msg(message_class, level, **kwargs)
        self.socket.send(self.serializer(message.__dict__), zmq.NOBLOCK, copy=True)  # copy=True faster for small msgs
        return message
