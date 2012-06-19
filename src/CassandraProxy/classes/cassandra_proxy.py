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
        #TODO: Check doppeltes Topic
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)

        #self.metadata.insert(topic, {'tablename' : topic, 'msg_class' : msg_class});
        
        self.subscriberList[str(topic)] = rospy.Subscriber(real_topic, msg_class, self.insertCassandra, topic)
        print "Added topic: " + str(topic)

    def removeTopic(self,topic):
        self.subscriberList[str(topic)].unregister()
        del self.subscriberList[str(topic)]
        print "Removed topic: " + str(topic)

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

        #comparator = CompositeType(LongType(reversed=True), AsciiType())
        #sys.create_column_family(keyspace, "CF1", comparator_type=comparator)
    def __playTopic(self, speed, topic, pub, starttime, endtime):
        
        messages = self.topictable.get(topic, column_start=starttime.to_sec(), column_finish=endtime.to_sec(), column_count=600);
        current_time = float(0.0)
        previous_time = float(0.0)
        for timestamp in messages.keys():

            return_object = yaml.load(messages[timestamp])
            pub.publish(return_object)
            if previous_time <= 0.0:
                delta_t = 0.0
            else:
                delta_t = timestamp - previous_time
            current_time += delta_t
            print "Timestamp: " + str(timestamp) + " Current Time: " + str(current_time)
            rospy.sleep(delta_t)
            previous_time = timestamp
        
        
    def playTopic(self, speed, topic, starttime, endtime):
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
        pub = rospy.Publisher(real_topic, msg_class)
        #self.__playTopic(speed, topic, pub, starttime, endtime)
        print "Playing topic: " + topic + " from Timestamp" + str(starttime.to_sec()) + " to " + str(endtime.to_sec())
        thread.start_new_thread(self.__playTopic, (speed, topic, pub, starttime, endtime))


#   ROS (Python)    Cassandra
#-------------------------------
#   string          AsciiType
#   unicode (NoSupp)UTF8Type
#   string          BytesType
#   long            LongType
#   long (int)      IntegerType (a generic variable-length integer type)
#   float           DoubleType
#   float           FloatType
#   -               LexicalUUIDType
#   rospy.Time      DateType

#http://www.ros.org/wiki/msg
#http://pycassa.github.com/pycassa/api/pycassa/types.html

