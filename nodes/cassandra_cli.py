#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')
import sys
import rospy
import time
import argparse

from CassandraProxy.srv import *


def __record(args):
    if args.start_stop=='start':
        args.start_stop=True
    else :
        args.start_stop=False
    rospy.wait_for_service('CassandraProxyRecord')
    try:
        recordCall = rospy.ServiceProxy('CassandraProxyRecord', record)
        status = recordCall(args.start_stop ,args.topic, rospy.Time.from_sec(float(args.begin)), rospy.Time.from_sec(float(args.end)))
        return status
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e


def __play(args):
    if args.start_stop=='start':
        args.start_stop=True
    else :
        args.start_stop=False
    rospy.wait_for_service('CassandraProxyPlay')
    rospy.sleep(args.delay)
    try:
        playCall = rospy.ServiceProxy('CassandraProxyPlay', play)
        while 1:
            status = playCall(args.start_stop, args.speed, args.topic, rospy.Time.from_sec(float(args.begin)), rospy.Time.from_sec(float(args.end)))
            if not args.loop:
                break
        return status
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

def __delete(args):
    rospy.wait_for_service('CassandraProxyCommand')
    try:
        commandCall = rospy.ServiceProxy('CassandraProxyCommand', command)
        commandCall('delete', args.topic, rospy.Time.from_sec(float(args.begin)), rospy.Time.from_sec(float(args.end)))
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e


def __info(args):
    rospy.wait_for_service('CassandraProxyCommand')
    try:
        commandCall = rospy.ServiceProxy('CassandraProxyCommand', command)
        print commandCall('info', args.topic, rospy.Time.from_sec(float(0)), rospy.Time.from_sec(float(0)))
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    
    recordparser = subparsers.add_parser('record')
    recordparser.set_defaults(func=__record)
    recordparser.add_argument("start_stop", help="start/stop recording", choices={'start','stop'})
    recordparser.add_argument("-b", "--begin", help="Start record/play at given time as float", type=float, default=1.0)
    recordparser.add_argument("-e", "--end", help="Stop record/play at given time as float", type=float,  default=4294967295.0)
    recordparser.add_argument("topic", metavar='TOPIC', nargs='+', help="Topic you want to record. Example: /turtle1/command_velocity")
    
    playparser = subparsers.add_parser('play')
    playparser.set_defaults(func=__play)
    playparser.add_argument("start_stop", help="start/stop playback", choices={'start','pause','stop'})
    playparser.add_argument("-s", "--speed", help="Playback speed as float. Default: 1.0", type=float, default=1.0)
    playparser.add_argument("-q", "--queue", help="Queue", type=int, default=100)
    playparser.add_argument("-d", "--delay", help="Delay in seconds", type=int, default=0)
    playparser.add_argument("-l", "--loop", help="Loop playback", action='store_true')
    playparser.add_argument("-b", "--begin", help="Start record/play at given time as float", type=float, default=1.0)
    playparser.add_argument("-e", "--end", help="Stop record/play at given time as float", type=float,  default=4294967295.0)
    playparser.add_argument("topic", metavar='TOPIC', nargs='+', help="Topic you want to play. Example: /turtle1/command_velocity")    
   
    deleteparser = subparsers.add_parser('delete')
    deleteparser.set_defaults(func=__delete)
    deleteparser.add_argument("-b", "--begin", help="Start record/play at given time as float", type=float, default=1.0)
    deleteparser.add_argument("-e", "--end", help="Stop record/play at given time as float", type=float,  default=4294967295.0)
    deleteparser.add_argument("topic", metavar='TOPIC', nargs='*', help="Topic you want to delete. Example: /turtle1/command_velocity")
    
    infoparser = subparsers.add_parser('info')
    infoparser.set_defaults(func=__info)
    infoparser.add_argument("topic", metavar='TOPIC', nargs='*', help="Topic you want info. Example: /turtle1/command_velocity")
    
    args = parser.parse_args()
    
    args.func(args)
