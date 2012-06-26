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
    return rospy.Time.now(),0

def commandCall(param):
    returnString = ""
    if len(param.topics)>0:
        for topic in param.topics:
            if param.command == 'info':
                returnString += cassandraproxy.infoTopic(topic)
            elif param.command == 'delete':
                cassandraproxy.deleteTopic(topic, param.starttime, param.endtime)
    else :
        if param.command == 'info' :
            returnString += cassandraproxy.infoMeta()
        elif param.command == 'delete' :
            cassandraproxy.deleteAllTopics()
    return returnString


def init():
    global cassandraproxy
    rospy.init_node('CassandraProxyService')
        
    host     = rospy.get_param("~host", "127.0.0.1")
    port     = int(rospy.get_param("~port", 9160))
    keyspace = rospy.get_param("~keyspace", "logging")
    cassandraproxy = CassandraProxy(host,port,keyspace)
    rospy.Service('CassandraProxyRecord', record, recordCall)
    rospy.Service('CassandraProxyPlay', play, playCall)
    rospy.Service('CassandraProxyCommand', command, commandCall)
    print "Service initialised"
    

if __name__ == "__main__":
    init()
    rospy.spin()
