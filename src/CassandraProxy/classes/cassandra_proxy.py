#!/usr/bin/env python
import roslib; roslib.load_manifest("CassandraProxy")
import roslib.message
import rospy
import rosmsg
import rostopic
import pycassa
from pycassa.system_manager import *
import yaml
from std_msgs.msg import String
from pycassa.types import *
import time
import hashlib
import uuid
import thread

class CassandraProxy:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.createKeyspace()
        self.pool = pycassa.ConnectionPool(self.keyspace, [self.host + ":" + str(self.port)])
        self.counter = 0
        try :
            self.metadata = pycassa.ColumnFamily(self.pool, 'metadata')
        except :
            self.createTable('metadata')
            self.metadata = pycassa.ColumnFamily(self.pool, 'metadata')
        try :
            self.topictable = pycassa.ColumnFamily(self.pool, 'data')
        except :
            self.createTable('data', comp_type=DoubleType())
            self.topictable = pycassa.ColumnFamily(self.pool, 'data')
            
        self.subscriberList = {};

    def addTopic(self,topic, starttime, endtime):
        if not self.subscriberList.has_key(str(topic)) :
            msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)

            self.metadata.insert(str(topic), {'tablename' : str(topic), 'msg_class' : str(msg_class)});
            
            self.subscriberList[str(topic)] = rospy.Subscriber(real_topic, msg_class, self.insertCassandra, topic)
            print "Added topic: " + str(topic)
        else : 
            print "Topic \"" + str(topic) +"\" already exists"

    def removeTopic(self,topic):
        if self.subscriberList.has_key(str(topic)) :
            self.subscriberList[str(topic)].unregister()
            del self.subscriberList[str(topic)]
            print "Removed topic: " + str(topic)
        else :
            print "Cant removed topic: " + str(topic) + ". It doesnt exist!"

    def insertCassandra(self,data, topic):
        time = rospy.get_time()
        self.topictable.insert(topic, {time: str(yaml.dump(data))})

        #DEBUG
        self.counter += 1
        print "EVENT", self.counter

    def spin(self):
        rospy.spin()

    def dispose(self):
        self.pool.dispose()

    def createKeyspace(self):
        sys = SystemManager(str(self.host) + ":" + str(self.port),framed_transport=True, timeout=30)
        try:
            print "Trying to create new keyspace"
            sys.create_keyspace(self.keyspace, SIMPLE_STRATEGY, {'replication_factor': '1'})
        except :
            print "Keyspace already exist."
        sys.close()


    def createTable(self, name, comp_type=None):
        sys = SystemManager(str(self.host) + ":" + str(self.port),framed_transport=True, timeout=30)
        try :
            print "Trying to create Table: " + str(name)
            sys.create_column_family(self.keyspace, str(name), comparator_type=comp_type)
        except :
            print "Drop and create new Table: " + str(name)
            sys.drop_column_family(self.keyspace, str(name))
            sys.create_column_family(self.keyspace, str(name),comparator_type=comp_type)
            
        sys.close()

    def __playTopic(self, speed, topic, pub, starttime, endtime):
        
        current_time = float(0.0)
        previous_time = starttime.to_sec()
        while True :
            messages = self.topictable.get(topic, column_start=previous_time, column_finish=endtime.to_sec(), column_count=100);
            if len(messages) == 0:
                break
            
            for timestamp in messages.keys():

                return_object = yaml.load(messages[timestamp])
                pub.publish(return_object)
                if current_time <= 0.0:
                    delta_t = 0.0
                else:
                    delta_t = (timestamp - previous_time) / speed
                current_time += delta_t
                print "Timestamp: " + str(timestamp) + " Current Time: " + str(current_time)
                rospy.sleep(delta_t)
                previous_time = timestamp
        
        
    def playTopic(self, speed, topic, starttime, endtime):
        #TODO: Check if topic exists
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
        pub = rospy.Publisher(real_topic, msg_class)
        #self.__playTopic(speed, topic, pub, starttime, endtime)
        print "Playing topic: " + topic + " from Timestamp" + str(starttime.to_sec()) + " to " + str(endtime.to_sec())
        thread.start_new_thread(self.__playTopic, (speed, topic, pub, starttime, endtime))
        
    def stopPlayTopic(self, topic):
        print "Not implemented yet"
    
    def pausePlayTopic(self, topic):
        print "Not implemented yet"
    
    def deleteTopic(self, topic, starttime, endtime):
        self.topictable.remove(str(topic), column_start=starttime.to_sec(), column_finish=endtime.to_sec())
    
    def deleteAllTopics(self):
        self.topictable.truncate()

    def infoMeta(self):
        returnString = "Current active Topics: "
        for recodingTopic in self.subscriberList.keys()
            returnString += recodingTopic + ", "
        returnString += "\n"
        returnString = "Recorded Topics: \n"
        recordedTopics = self.metadata.get_range()
        for topicName, columns in result:
            returnString += "\t" + str(key) + "=>" + str(columns)
        returnString += "\n"
        return returnString
    
    def infoTopic(self, topic):
        returnString = "Topic: " + topic "\n"
        returnString += "\t Number of Messages: " + str(self.topictable.get_count(topic))
        returnString += "\t Starttime, Endtime: " + str(self.topictable.get(topic, column_count=1)) + "=>" + str(self.topictable.get(topic, column_reversed=True, column_count=1)
        returnString += "\n"
        return returnString
