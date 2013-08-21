__author__ = 'tjohnson'

import outlog.msgs
import outlog.factory
import time
import ujson

if __name__ == "__main__":
    num_messages = 1000000
    print "Creating %s heartbeat messages" % num_messages

    file_handle = open("TestJsonFileWriting.out", "wb")

    factory = outlog.factory.SerializedFileMessageFactory(
        hostname="host", application="PerformanceTests",
        file_handle=file_handle, serializer=ujson.dumps)

    start_time = time.time()
    for i in xrange(0, num_messages):
        x = factory.msg(outlog.msgs.Heartbeat, "DEBUG")

    file_handle.close()
    end_time = time.time()

    print "Total time: %s" % (end_time - start_time)
    print "Microsecs per message: %s" % (((end_time-start_time) / num_messages) * 1000. * 1000.)
