#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')
import sys
import rospy
import time
import argparse

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


# info allgemein ueber alles, ueber topic ...
# info cluster / cassandra
# play _pause, delay, loop 
#  --queue=SIZE          use an outgoing queue of size SIZE (defaults to 100)
# --topics              "the list topics to play back"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Either record or play a topic", choices={'record','play','delete', 'info'})
    parser.add_argument("start_stop", help="start/stop recording/playback ", choices={'start','stop'})
    parser.add_argument("topic", help="Topic you want to record. Example: /turtle1/command_velocity")
    parser.add_argument("-s", "--speed", help="Playback speed as float. Default: 1.0", type=float, default=1.0)
    parser.add_argument("-b", "--begin", help="Start record/play at given time as float", type=float, default=1.0)
    parser.add_argument("-e", "--end", help="Stop record/play at given time as float", type=float,  default=4294967295.0)
    args = parser.parse_args()
    
    
    
    if args.start_stop == 'start' :
        start = True
    else :
        start = False
    
    if args.command == 'record' :
        callRecordService(start, args.topic, args.begin, args.end)
    else :
        callPlayService(start, args.speed, args.topic, args.begin, args.end)
