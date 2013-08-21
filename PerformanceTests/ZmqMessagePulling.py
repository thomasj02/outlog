
__author__ = 'tjohnson'

import outlog.msgs
import outlog.factory
import outlog.consumer
import time
import ujson
import sys
import msgpack
import zmq

if __name__ == "__main__":
    num_messages = 1000000
    print "Receiving %s heartbeat messages" % num_messages

    context = zmq.Context(1)
    socket = context.socket(zmq.PULL)
    socket.bind(sys.argv[1])

    deserializer = None
    if sys.argv[2] == "ujson":
        deserializer = ujson.loads
    elif sys.argv[2] == "msgpack":
        deserializer = msgpack.loads

    decoder = outlog.consumer.SerializedOutlogMessageDecoder(
        {"Heartbeat": outlog.msgs.Heartbeat},
        deserializer=deserializer)
    consumer = outlog.consumer.ZmqToOutlogMessageDecoder(socket=socket, serialized_outlog_msg_decoder=decoder)

    start_time = time.time()

    messages_received = 0
    while messages_received < num_messages:
        x = consumer.consume()
        if x:
            messages_received += 1

    socket.close()
    context.destroy()
    end_time = time.time()

    print "Total time: %s" % (end_time - start_time)
    print "Microsecs per message: %s" % (((end_time - start_time) / num_messages) * 1000. * 1000.)
