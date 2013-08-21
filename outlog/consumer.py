import ujson
import zmq

__author__ = 'tjohnson'


class SerializedFileWriter(object):
    def __init__(self, file_handle, serializer=ujson.dumps):
        """
        Instantiate a consumer that writes json-encoded messages to a file

        :param file_handle: A file handle that supports write()
        :param serializer: The serializer function, which returns the serialized data when given a message
        """
        self.file_handle = file_handle
        self.serializer = serializer

    def consume(self, outlog_message):
        """
        Encode a message using the serialization function and write it to a file

        :type outlog_message: msgs.BaseMessage
        :param outlog_message: An outlog logging message
        """
        self.file_handle.write(self.serializer(outlog_message))
        self.file_handle.write("\n")

    def consume_serialized_msg(self, serialized_outlog_message):
        """
        Write an already json-encoded message to a file

        :type serialized_outlog_message: str
        :param serialized_outlog_message: An outlog logging message, json-encoded
        """
        self.file_handle.write(serialized_outlog_message)
        self.file_handle.write("\n")

class HumanFileWriter(object):
    def __init__(self, file_handle):
        """
        Instantiate a consumer that writes message's __repr__ to a file

        :param file_handle: A file handle that supports write()
        """
        self.file_handle = file_handle

    def consume(self, outlog_message):
        """
        Write the message to a file

        :type outlog_message: msgs.BaseMessage
        :param outlog_message: An outlog logging message
        """
        self.file_handle.write(outlog_message)
        self.file_handle.write("\n")

class SerializedOutlogMessageDecoder(object):
    def __init__(self, message_names_to_classes=dict(), deserializer=ujson.loads):
        self.message_names_to_classes = message_names_to_classes
        self.deserializer = deserializer

    def add_message_to_class_mapping(self, message_name, class_):
        self.message_names_to_classes[message_name] = class_

    def consume_serialized(self, serialized_outlog_msg):
        message_dict = self.deserializer(serialized_outlog_msg)
        message_class = self.message_names_to_classes[message_dict["message_name"]]
        message = message_class(**message_dict)
        return message

class ZmqToOutlogMessageDecoder(object):
    def __init__(self, socket, serialized_outlog_msg_decoder):
        """
        Instantiate a consumer that receives JSON messages via 0MQ and decodes them to outlog messages
        Note: You must configure the mappings in the serialized_outlog_msg_decoder for this to work properly

        :type socket: zmq.Socket
        :type serialized_outlog_msg_decoder: SerializedOutlogMessageDecoder
        :return: A decoded message, or None if no messages are available
        """
        self.socket = socket
        self.serialized_decoder = serialized_outlog_msg_decoder

    def consume(self):
        try:
            serialized_message = self.socket.recv(flags=zmq.NOBLOCK, copy=True)
            message = self.serialized_decoder.consume_serialized(serialized_message)
            return message

        except zmq.Again:
            return None
