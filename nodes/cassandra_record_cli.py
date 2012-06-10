#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')

import sys

import rospy
import time

from CassandraProxy.srv import *

def callRecordService(doRecord, topic, starttime, endtime):
	
    rospy.wait_for_service('CassandraProxyRecord')
    try:
        recordCall = rospy.ServiceProxy('CassandraProxyRecord', record)
        status = recordCall(doRecord ,topic, rospy.Time.from_sec(float(starttime)), rospy.Time.from_sec(float(endtime)))
        return status
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

def callPlayService(doPlay, speed, topic, starttime, endtime):
	
    rospy.wait_for_service('CassandraProxyPlay')
    try:
        playCall = rospy.ServiceProxy('CassandraProxyPlay', play)
        status = playCall(doPlay, speed, topic, rospy.Time.from_sec(float(starttime)), rospy.Time.from_sec(float(endtime)))
        return status
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

def usage():
    return "%s (record/play) (start/stop) topic [starttime endtime]"%sys.argv[0]
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
		if sys.argv[1]=="record":
			if sys.argv[2]=="start":
				callRecordService(True, sys.argv[3],sys.argv[4],sys.argv[5])
			else:
				callRecordService(False, sys.argv[3],sys.argv[4],sys.argv[5])
		if sys.argv[1]=="play":
			if sys.argv[2]=="start":
				callPlayService(True, 1.0, sys.argv[3],sys.argv[4],sys.argv[5])
			else:
				callPlayService(False, 1.0, sys.argv[3],sys.argv[4],sys.argv[5])
		else:
			usage()
    else:
        print usage()
