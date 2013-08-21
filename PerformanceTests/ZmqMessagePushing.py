__author__ = 'tjohnson'

import msgpack
import zmq
import outlog.msgs
import outlog.factory
import time
import ujson
import sys

if __name__ == "__main__":
    num_messages = 1000000
    print "Creating %s heartbeat messages" % num_messages

    context = zmq.Context(1)
    socket = context.socket(zmq.PUSH)
    socket.connect(sys.argv[1])

    serializer = None
    if sys.argv[2] == "ujson":
        serializer = ujson.dumps
    elif sys.argv[2] == "msgpack":
        serializer = msgpack.dumps

    factory = outlog.factory.ZmqMessageFactory(
        hostname="host", application="PerformanceTests",
        socket=socket, serializer=serializer)

    start_time = time.time()
    for i in xrange(0, num_messages):
        x = factory.msg(outlog.msgs.Heartbeat, "DEBUG")

    socket.close()
    context.destroy()
    end_time = time.time()

    print "Total time: %s" % (end_time - start_time)
    print "Microsecs per message: %s" % (((end_time-start_time) / num_messages) * 1000. * 1000.)
