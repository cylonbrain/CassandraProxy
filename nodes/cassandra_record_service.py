#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')

from CassandraProxy.srv import *
from CassandraProxy.classes import *
import rospy

cassandraproxy = None

def recordCall(param):
    if param.record==True:
        cassandraproxy.addTopic(param.topic, param.starttime, param.endtime)
        return 1
    else:
        cassandraproxy.removeTopic(param.topic)
        return 0

def playCall(param):
    if param.play==True:
        cassandraproxy.playTopic(param.speed, param.topic, param.starttime, param.endtime)
        return rospy.get_rostime(),1
    else:
        cassandraproxy.stopPlayTopic(param.topic)
        return None, 0

def init():
    global cassandraproxy
    rospy.init_node('CassandraProxyService')
        
    host     = rospy.get_param("~host", "127.0.0.1")
    port     = int(rospy.get_param("~port", 9160))
    keyspace = rospy.get_param("~keyspace", "logging")
    topic = rospy.get_param("~topic", "/turtle1/command_velocity")
    cassandraproxy = CassandraProxy(host,port,keyspace)

    r = rospy.Service('CassandraProxyRecord', record, recordCall)
    p = rospy.Service('CassandraProxyPlay', play, playCall)
    print "Service initialised"
    

if __name__ == "__main__":
    init()
    rospy.spin()
