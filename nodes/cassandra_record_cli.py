#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')

import sys

import rospy
from CassandraProxy.srv import *

def callService(topic):
    rospy.wait_for_service('CassandraProxyRecord')
    try:
        recordCall = rospy.ServiceProxy('CassandraProxyRecord', record)
        status = recordCall(True,topic, None, None)
        return status
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

def play(topic):
    rospy.wait_for_service('CassandraProxyRecord')
    try:
        return None
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

def usage():
    return "%s (record/play) [stop] topic starttime endtime"%sys.argv[0]
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        callService(sys.argv[2])
    else:
        print usage()
