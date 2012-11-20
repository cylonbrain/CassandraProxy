#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import Connection
import roslib; roslib.load_manifest("CassandraProxy")
import roslib.message
import rospy
import rosmsg
import rostopic
import yaml
from std_msgs.msg import String
import time

class MongoRecorder(object):
    """docstring for MongoRecorder"""
    def __init__(self, host, port):
        global cassandraproxy
        rospy.init_node('MongoRecorder')
        self.connection = Connection(host, port)
        self.db = self.connection.test_database
        self.collection = self.db.test_collection
        self.messages = self.collection.messages


    def record(self, topic):
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
        rospy.Subscriber(real_topic, msg_class, self.__insertMongo, topic)

    def __insertMongo(self, data, topic):
        time = rospy.get_time()
        message = {"topic": topic,
                   "time" : time,
                   "data" : str(yaml.dump(data))}
        self.messages.insert(message)
        print "Added " + str(message)
        
    def play(self, topic):
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
        print msg_class
        print real_topic
        pub = rospy.Publisher(real_topic, msg_class)
        delta_t=0.0;
        previous_time=0.0
        for message in self.messages.find():
            timestamp = message["time"]
            return_object = yaml.load(message["data"])
            pub.publish(return_object)
            if previous_time == 0:
                    delta_t = 0.0
            else:
                    delta_t = (timestamp - previous_time)
            rospy.sleep(delta_t)
            previous_time = timestamp
            print "Published: \n" + str(message["data"])

if __name__ == "__main__":
    topic = "/turtle1/command_velocity"
    recorder = MongoRecorder('localhost', 27017)
    #recorder.record(topic)
    recorder.play(topic)
    rospy.spin()
