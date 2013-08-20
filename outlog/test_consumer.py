from unittest import TestCase
import ujson
import consumer
import msgs

__author__ = 'tjohnson'

class TestMessage(msgs.BaseMessage):
    def __init__(self, hostname, microtime, application, level, message_name="Test", extra_data="foo"):
        super(TestMessage, self).__init__(
            hostname=hostname, microtime=microtime, application=application, level=level, message_name=message_name)
        self.extra_data = extra_data

class TestJsonToOutlogMessageDecoder(TestCase):
    def setUp(self):
        self.sut = consumer.JsonToOutlogMessageDecoder()

    def test_consume_json(self):
        test_message = TestMessage(hostname="host", microtime=123, application="app", level="DEBUG")
        self.sut.add_message_to_class_mapping("Test", TestMessage)

        json_test_message = ujson.dumps(test_message)
        decoded_message = self.sut.consume_json(json_test_message)

        self.assertIsInstance(decoded_message, TestMessage)
        self.assertEqual(test_message.__dict__, decoded_message.__dict__)
