#!/usr/bin/env python
 
import sys, time

from tooz import coordination

import daemon

BACKEND_URL = 'zookeeper://0.0.0.0:2181' 

class Coordinator(daemon.Daemon):
    def __init__(self, host):
        self.coordinator = coordination.get_coordinator(BACKEND_URL, host)
        super(self.__class__, self).__init__('/tmp/coord_%s' % host, '/dev/null',
                                             '/tmp/coord_%s_out.log' % host,
                                             '/tmp/coord_%s_err.log' % host)

    def start(self):
        self.coordinator.start()

        group = bytes("inspector")
        request = self.coordinator.get_members(group)
        try:
            request.get()
        except coordination.GroupNotCreated:
            request = self.coordinator.create_group(group)

        self.coordinator.join_group(group).get()
        members = self.coordinator.get_members(group).get()
        print("Group members: %s" % members)

        super(self.__class__, self).start()

    def stop(self):
        #self.coordinator.stop()
        super(self.__class__, self).stop()

    def run(self):
        while True:
            start = time.time()
            while time.time() - start < 3:
                self.coordinator.heartbeat()
                time.sleep(0.2)
            time.sleep(1)
 
if __name__ == "__main__":
        #coord1 = Coordinator('/tmp/coord1.pid', b'test1')
        #import pdb; pdb.set_trace()
        if len(sys.argv) == 3:
            coord1 = Coordinator(bytes(sys.argv[2]))
            if 'start' == sys.argv[1]:
                coord1.start()
            elif 'stop' == sys.argv[1]:
                    coord1.stop()
                    #coord2.stop()
            elif 'restart' == sys.argv[1]:
                    coord1.restart()
                    #coord2.restart()
            else:
                    print "Unknown command"
                    sys.exit(2)
            sys.exit(0)
        else:
            print "usage: %s start|stop|restart <host name>" % sys.argv[0]
            sys.exit(2)

