import ujson
import zmq

__author__ = 'tjohnson'


class JsonFileWriter(object):
    def __init__(self, file_handle):
        """
        Instantiate a consumer that writes json-encoded messages to a file

        :param file_handle: A file handle that supports write()
        """
        self.file_handle = file_handle

    def consume(self, outlog_message):
        """
        Encode a message using json and write it to a file

        :type outlog_message: msgs.BaseMessage
        :param outlog_message: An outlog logging message
        """
        self.file_handle.write(ujson.dumps(outlog_message))
        self.file_handle.write("\n")

    def consume_json(self, json_outlog_message):
        """
        Write an already json-encoded message to a file

        :type json_outlog_message: str
        :param json_outlog_message: An outlog logging message, json-encoded
        """
        self.file_handle.write(json_outlog_message)
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

class JsonToOutlogMessageDecoder(object):
    def __init__(self, message_names_to_classes=dict()):
        self.message_names_to_classes = message_names_to_classes

    def add_message_to_class_mapping(self, message_name, class_):
        self.message_names_to_classes[message_name] = class_

    def consume_json(self, json_outlog_message):
        message_dict = ujson.loads(json_outlog_message)
        message_class = self.message_names_to_classes[message_dict["message_name"]]
        message = message_class(**message_dict)
        return message

class ZmqToOutlogMessageDecoder(object):
    def __init__(self, socket, json_decoder):
        """
        Instantiate a consumer that receives JSON messages via 0MQ and decodes them to outlog messages
        Note: You must configure the mappings in the json_decoder for this to work properly

        :type socket: zmq.Socket
        :type json_decoder: JsonToOutlogMessageDecoder
        :return: A decoded message, or None if no messages are available
        """
        self.socket = socket
        self.json_decoder = json_decoder

    def consume(self):
        try:
            json_message = self.socket.recv(flags=zmq.NOBLOCK, copy=True)
            message = self.json_decoder.consume_json(json_message)
            return message

        except zmq.Again:
            return None