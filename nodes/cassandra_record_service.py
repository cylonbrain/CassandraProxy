#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')

from CassandraProxy.srv import *
from CassandraProxy.classes import *
import rospy

cassandraproxy = None

def recordCall(param):
    for topic in (param.topics):
        if param.record==True:
            cassandraproxy.addTopic(topic, param.starttime, param.endtime)
        else:
            cassandraproxy.removeTopic(topic)
    return 0


def playCall(param):
    for topic in param.topics:
        if param.play==True:
            cassandraproxy.playTopic(param.speed, topic, param.starttime, param.endtime)
        else:
            cassandraproxy.stopPlayTopic(topic)
    return 0


def init():
    global cassandraproxy
    rospy.init_node('CassandraProxyService')
        
    host     = rospy.get_param("~host", "127.0.0.1")
    port     = int(rospy.get_param("~port", 9160))
    keyspace = rospy.get_param("~keyspace", "logging")
    cassandraproxy = CassandraProxy(host,port,keyspace)
    r = rospy.Service('CassandraProxyRecord', record, recordCall)
    p = rospy.Service('CassandraProxyPlay', play, playCall)
    print "Service initialised"
    

if __name__ == "__main__":
    init()
    rospy.spin()
