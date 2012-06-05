#!/usr/bin/env python
import roslib; roslib.load_manifest('CassandraProxy')

from CassandraProxy.srv import *
from CassandraProxy.classes import *
import rospy

cassandraproxy = None

def recordCall(param):
    if param.record==True:
        cassandraproxy.addTopic(param.topic)
        cassandraproxy.spin()
        return 1
    else:
        cassandraproxy.removeTopic(param.topic)
        cassandraproxy.dispose()
        return 0

def playCall(param):
    print "Not implemented yet"
    rospy.spin()

def init():
    global cassandraproxy
    rospy.init_node('CassandraProxy')
        
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
