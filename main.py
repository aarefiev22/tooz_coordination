#!/usr/bin/env python
 
import sys, time

from tooz import coordination

import daemon

BACKEND_URL = 'zookeeper://0.0.0.0:2181' 

class Coordinator(daemon.Daemon):
    def __init__(self, host):
        self.coordinator = coordination.get_coordinator(BACKEND_URL, host, timeout=10, namespace="tooz")
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

        # create job board
        board = self.coordinator.make_job_board('test1')
        job = board.post('job1').get()
        print("Posted job: %s" % job.name)
        jobs_it = board.iterjobs()
        list_jobs = []
        for job in jobs_it:
             list_jobs.append(job.name())
        print("Created jobs: %s" % ' '.join(list_jobs))
            
        #super(self.__class__, self).start()

    def stop(self):
        #self.coordinator.stop()
        super(self.__class__, self).stop()

    def run(self):
        print("Running coordinator %s" % self.coordinator._member_id)
        while True:
            print("Hearbeat")
            timeout = self.coordinator.heartbeat()
            print("Timeout hearbeat: %s" % timeout)
            #print("Fetch board")
            #board = self.coordinator.fetch_job_board('test1')
            #print("Get iterators")
            #jobs_it = board.iterjobs()
            #print("job count: %s in %s " % (board.job_count(), board.name()))
            #with open("/tmp/test1_board.log", "a") as log:
            #    for job in jobs_it:
            #        log.write("Succsesfully fetched job: %s" % job.name)
            time.sleep(3)
 
if __name__ == "__main__":
        #coord1 = Coordinator('/tmp/coord1.pid', b'test1')
        #import pdb; pdb.set_trace()
        if len(sys.argv) == 3:
            coord1 = Coordinator(bytes(sys.argv[2]))
            if 'start' == sys.argv[1]:
                coord1.start()
                coord1.run()
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

