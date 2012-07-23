#!/usr/bin/env python
# -*- coding: utf-8 -*-
import roslib; roslib.load_manifest('CassandraProxy')

from CassandraProxy.srv import *
from CassandraProxy.classes import *
import rospy

cassandraproxy = None

# Diese Klasse erstellt eine Instanz der Klasse "CassandraProxy". Es werden dabei drei verschiedenene Services registriert.

# * CassandraProxyRecord: Wird für die Steuerung der Aufzeichnung von Themen verwendet. 
# * CassandraProxyPlay: Wird für die Steuerung der Wiedergabe von Themen verwendet. 
# * CassandraProxyCommand: Wird für alle sonstigen Kommandos, die nicht in einen vorherigen Service aufrufe passt.

# Der Service läuft bis zu seiner Beendigung im Hintergrund. Alle Service aufrufe sind nicht blockierend implementiert.
# Es können drei Parameter beim Start angegeben werden:

#     1. host : Die Adresse der Cassandra Datenbank
#     2. port : Der Port unter dem die Cassandra Datenbank läuft
#     3. keyspace : Der gewünschte Keyspace

# Bsp.: ./cassandra_service.py localhost 9160 logging

# Werden keine Parameter angegeben werden die Standardwerte "localhost" "9160" "logging" verwendet.


def recordCall(param):
    for topic in (param.topics):
        if param.record==True:
            cassandraproxy.addTopic(topic, param.starttime, param.endtime)
        else:
            cassandraproxy.removeTopic(topic)
    return 0


def playCall(param):
    for topic in param.topics:
        if param.play=='start':
            cassandraproxy.playTopic(param.speed, topic, param.starttime, param.endtime)
        elif param.play=='pause':
            cassandraproxy.pausePlayTopic(topic)
        elif param.play=='stop':
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
