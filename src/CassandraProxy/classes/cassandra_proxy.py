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
        
        try :
            self.metadata = pycassa.ColumnFamily(self.pool, 'metadata')
        except :
            self.createTable('metadata')
            self.metadata = pycassa.ColumnFamily(self.pool, 'metadata')
            
        self.tableList = {};
        self.subscriberList = {};

    def addTopic(self,topic, starttime, endtime):
        #TODO: Check doppeltes Topic
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
        table = None
        tablename = hashlib.sha1(topic).hexdigest()
        
        try :
            table = pycassa.ColumnFamily(self.pool, tablename)
        except :
            self.createTable(tablename)
            table = pycassa.ColumnFamily(self.pool, tablename)

        self.metadata.insert(topic, {'tablename' : tablename});
        self.tableList[str(topic)] = table
        self.subscriberList[str(topic)] = rospy.Subscriber(real_topic, msg_class, self.insertCassandra, table)
        print "Added topic: " + str(topic) + " with Tablename: " + tablename

    def removeTopic(self,topic):
        self.subscriberList[str(topic)].unregister()
        del self.tableList[str(topic)]
        del self.subscriberList[str(topic)]
        print "Removed topic: " + str(topic)


    def insertCassandra(self,data, table):
        time = rospy.get_time()
        table.insert(str(time), {'yaml_object': str(yaml.dump(data))})
        
        #DEBUG
        print "EVENT"

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
        
    def playTopic(self, speed, topic, starttime, endtime):
        msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
        tablename = hashlib.sha1(topic).hexdigest()
        pub = rospy.Publisher(real_topic, msg_class)
        try :
            table = pycassa.ColumnFamily(self.pool, tablename)
        except :
            print "No matching table to topic"
            return
        
        messages = table.get_range()#start=str(starttime), finish=str(endtime));
        
        for key, message in messages:
            return_object = yaml.load(message['yaml_object'])
            pub.publish(return_object)
            
            #DEBUG
            print key + " => " + str(return_object)
            rospy.sleep(1.0)
    
        

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

