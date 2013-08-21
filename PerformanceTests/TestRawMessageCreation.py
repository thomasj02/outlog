__author__ = 'tjohnson'

import outlog.msgs
import outlog.factory
import time

if __name__ == "__main__":
    num_messages = 1000000
    print "Creating %s heartbeat messages" % num_messages

    factory = outlog.factory.MessageFactory(hostname="host", application="PerformanceTests")

    start_time = time.time()
    for i in xrange(0, num_messages):
        x = factory.msg(outlog.msgs.Heartbeat, "DEBUG")

    end_time = time.time()

    print "Total time: %s" % (end_time - start_time)
    print "Microsecs per message: %s" % (((end_time-start_time) / num_messages) * 1000. * 1000.)
