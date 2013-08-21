from unittest import TestCase
import ujson
import zmq
import consumer
import msgs

__author__ = 'tjohnson'


class TestMessage(msgs.BaseMessage):
    def __init__(self, hostname, microtime, application, level, message_name="Test", extra_data="foo"):
        super(TestMessage, self).__init__(
            hostname=hostname, microtime=microtime, application=application, level=level, message_name=message_name)
        self.extra_data = extra_data


class TestSerializedOutlogMessageDecoder(TestCase):
    def setUp(self):
        self.sut = consumer.SerializedOutlogMessageDecoder()

    def test_consume_json(self):
        test_message = TestMessage(hostname="host", microtime=123, application="app", level="DEBUG")
        self.sut.add_message_to_class_mapping("Test", TestMessage)

        json_test_message = ujson.dumps(test_message)
        decoded_message = self.sut.consume_serialized(json_test_message)

        self.assertIsInstance(decoded_message, TestMessage)
        self.assertEqual(test_message.__dict__, decoded_message.__dict__)


class TestZmqToOutlogMessageDecoder(TestCase):
    def setUp(self):
        self.json_decoder = consumer.SerializedOutlogMessageDecoder()
        self.json_decoder.add_message_to_class_mapping("Test", TestMessage)

        self.context = zmq.Context(1)
        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.bind("inproc://test")

        self.pull_socket = self.context.socket(zmq.PULL)
        self.pull_socket.connect("inproc://test")

        self.sut = consumer.ZmqToOutlogMessageDecoder(self.pull_socket, self.json_decoder)

    def test_consume_no_messages_available(self):
        decoded_message = self.sut.consume()
        self.assertIsNone(decoded_message)

    def test_consume(self):
        test_message = TestMessage(hostname="host", microtime=123, application="app", level="DEBUG")
        json_test_message = ujson.dumps(test_message)

        self.push_socket.send(json_test_message)

        decoded_message = self.sut.consume()
        self.assertIsInstance(decoded_message, TestMessage)
        self.assertEqual(test_message.__dict__, decoded_message.__dict__)

    def tearDown(self):
        self.push_socket.close()
        self.pull_socket.close()
        self.context.destroy()