#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from threading import *

# Mit der Klasse "CassandraProxy" können beliebige Topics aufgezeichnet und in 
# eine laufende Cassandrainstanz geschrieben werden. Außerdem können 
# aufgezeichnete Topics wiedergegeben werden. Diese Klasse übernimmt dabei die
# komplette Kommunikation mit der Cassandra Datenbank über das Objekt 
# "self.topictable", welches auf die Tabelle 'data' im angegebenen keyspace 
# referenziert. In dieser sind alle Nachrichten mit ihren jeweiligen Zeitstempen
# unter dem Topicnamen als Schlüssel gespeichert. Gleichzeitig wird in der 
# Tabelle 'metadata' ein Index über die Aufgezeichneten Topics gepflegt.

class CassandraProxy:
    def __init__(self, host, port, keyspace):
        """Initialisierung der CassandraProxy Klasse. Es wird eine Verbindung zu der Datenbank hergestellt. Außerdem werden die Methoden zum erstellen des jeweiligen Keyspace und der Tabellen aufgerufen."""
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
            
        self.subscriberList = {}
        self.playThreadList = {}

    def addTopic(self,topic, starttime, endtime):
        """Diese Methode registriert ein Topic für die Aufzeichnung in die Cassandra Datenbank. Außerdem werden Metadaten in eine separate Tabelle geschrieben"""
        #TODO: Anfangs und Endzeit berücksichtigen, mit thrading.Timer in eigenen Thread auslagern und diesen zum Zeitpunkt "endtime" automatisiert beenden.
        if not self.subscriberList.has_key(str(topic)) :
            msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)

            self.metadata.insert(str(topic), {'tablename' : str(topic), 'msg_class' : str(msg_class)})
            
            self.subscriberList[str(topic)] = rospy.Subscriber(real_topic, msg_class, self.__insertCassandra, topic)
            print "Added topic: " + str(topic)
        else : 
            print "Topic \"" + str(topic) +"\" already exists"

    def removeTopic(self,topic):
        """Diese Methode entfernt das Abbonement für das angegebene Topic."""
        if self.subscriberList.has_key(str(topic)) :
            self.subscriberList[str(topic)].unregister()
            del self.subscriberList[str(topic)]
            print "Removed topic: " + str(topic)
        else :
            print "Cant removed topic: " + str(topic) + ". It doesnt exist!"

    def __insertCassandra(self,data, topic):
        """Der Callback, der für jede Nachricht aufgerufen wird und die Nachricht als YAML-Objekt in die CassandraDB einfügt"""
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
        """Wenn noch kein passender Keyspace exisitert wird dieser hier erstellt"""
        sys = SystemManager(str(self.host) + ":" + str(self.port),framed_transport=True, timeout=30)
        try:
            print "Trying to create new keyspace"
            sys.create_keyspace(self.keyspace, SIMPLE_STRATEGY, {'replication_factor': '1'})
        except :
            print "Keyspace already exist."
        sys.close()


    def createTable(self, name, comp_type=None, super_table=False):
        """Erstellt Tabellen mit "name" in dem mittels createKeyspace erstellten Keyspace"""
        sys = SystemManager(str(self.host) + ":" + str(self.port),framed_transport=True, timeout=30)
        try :
            print "Trying to create Table: " + str(name)
            sys.create_column_family(self.keyspace, str(name), comparator_type=comp_type)
        except :
            print "Drop and create new Table: " + str(name)
            sys.drop_column_family(self.keyspace, str(name))
            sys.create_column_family(self.keyspace, str(name),comparator_type=comp_type)
            
        sys.close()

    def __playTopic(self, speed, topic, pub, starttime, endtime, allowPlaying, shoudStop):
        """Diese Methode läuft in einem eigenen Thread und publiziert das angegebene Topic. Aus Geschwindigkeitsgründen werden jeweils n-Nachrichten einzeln aus der Tabelle geholt"""
        current_time = float(0.0)
        previous_time = starttime.to_sec()
        timestamp = float(0.0)
        while True:
            #Idee: Hole jeweils n Elemente aus der Datenbank. Bei jedem durchlauf jeweils angefangen von dem vorherigen Zeitstempel.
            messages = self.topictable.get(topic, column_start=previous_time, column_finish=endtime.to_sec(), column_count=100)

            for timestamp in messages.keys():
                if shoudStop.is_set():
                    return
                #Thread kann hier unterbrochen werden um die pause funktionalität zu ermöglichen
                allowPlaying.wait()
                return_object = yaml.load(messages[timestamp])
                pub.publish(return_object)
                if previous_time == starttime.to_sec():
                    delta_t = 0.0
                else:
                    delta_t = (timestamp - previous_time) / speed
                current_time += delta_t
                print "Timestamp: " + str(timestamp) + " Current Time: " + str(current_time)
                rospy.sleep(delta_t)
                previous_time = timestamp + 0.00000001  
            if len(messages) == 1:
                break
        
        
    def playTopic(self, speed, topic, starttime, endtime):
        """Erstellt den neuen Thread für __playTopic"""
        if self.doesTopicExist(topic):
            msg_class, real_topic, _ = rostopic.get_topic_class(topic, blocking=True)
            pub = rospy.Publisher(real_topic, msg_class)
            #self.__playTopic(speed, topic, pub, starttime, endtime)
            print "Playing topic: " + topic + " from Timestamp" + str(starttime.to_sec()) + " to " + str(endtime.to_sec())

            #allowPlaying wird benötigt, damit das abspielen später pausiert werden kann
            allowPlaying = Event()
            shoudStop = Event()
            playThread = Thread(target=self.__playTopic, args=(speed, topic, pub, starttime, endtime, allowPlaying, shoudStop))
            #Standardmäßig dürfen alle Threads laufen, außerdem werden sie als Daemonthread gesetzt, da keine Daten verloren gehen wenn sie einfach beendet werden
            allowPlaying.set()
            playThread.setDaemon(True)
            self.playThreadList[str(topic)] = [playThread, allowPlaying, shoudStop]
            playThread.start()
        else:
            print "ERROR: Cant play Topic. Topic doesnt exist"
        
    def stopPlayTopic(self, topic):
        """Stopt das Publizieren für das angegebene Topic"""
        if self.playThreadList.has_key(str(topic)):
            self.playThreadList[str(topic)][2].set()
            del self.playThreadList[str(topic)]
        else:
            print topic + " isn't yet published"
    
    def pausePlayTopic(self, topic):
        """Startet und pausiert das Publizieren des angegebenen Topics"""
        print "Play/Pause: " + topic
        if self.playThreadList.has_key(str(topic)):
            allowPlaying = self.playThreadList[str(topic)][1]
            if allowPlaying.is_set():
                allowPlaying.clear()
            else:
                allowPlaying.set()
    
    def deleteTopic(self, topic, starttime, endtime):
        """Entfernt die Nachrichten des angegebenen Topics im Intervall von Start- bis Endzeit"""
        topic = str(topic)
        __, columns = self.topictable.get_range(topic,topic, cloumn_start=starttime.to_sec(), column_finish=endtime.to_sec())
        self.topictable.remove(topic, columns)
        if self.topictable.get_count(topic) == 0:
            self.metadata.remove(topic)
    
    def deleteAllTopics(self):
        """Löscht die Topictabelle vollständig. Damit werden alle Nachrichten aller Topics entfernt"""
        self.topictable.truncate()

    def infoMeta(self):
        """Gibt Metainformationen über die aktuell in der Aufzeichnung befindlichen Topics zurück. Außerdem wird ein Listing über die in der Cassandra DB vorhandenen Topics zurückgegeben"""
        returnString = "Current active Topics: "
        for recodingTopic in self.subscriberList.keys():
            returnString += recodingTopic + ", "
        returnString += "\n"
        returnString += "Recorded Topics: \n"
        recordedTopics = self.metadata.get_range()
        for topicName, columns in recordedTopics:
            returnString += "\t" + str(topicName) + " => " + str(columns['msg_class'])
        returnString += "\n"
        return returnString
    
    def infoTopic(self, topic):
        """Gibt Metainformationen für ein einzelnes Topic wieder. Es werden die Anzahl der zum Topic gehörenden Nachrichten und der Zeitintervall von dem Nachrichten vorhanden sind zurückgegeben und """
        returnString = "Topic: " + topic + "\n"
        returnString += "\t Number of Messages: " + str(getNumberOfMessages(topic))
        start, end = self.getTimeIntervall(topic)
        returnString += "\t Starttime, Endtime: " + str(start) + "=>" + str(end)
        returnString += "\n"
        returnString += "\t Starttime, Endtime: " + str(self.topictable.get(topic, column_count=1).keys()) + " => " + str(self.topictable.get(topic, column_reversed=True, column_count=1).keys())
        returnString += "\n"
        return returnString

    def getNumberOfMessages(self, topic):
        """Anzahl der Nachrichten für topic"""
        return self.topictable.get_count(topic)
    
    def getTimeIntervall(self, topic):
        """Gibt ein zweielementiges Array mit Startzeit und Endzeitpunkt für das angegebene Topic zurück. Der Startzeit ist dabei der früheste gespeicherte Datensatz. Der Endzeitpunkt der letzte gespeicherte Datensatz."""
        return  array(self.topictable.get(topic, column_count=1), self.topictable.get(topic, column_reversed=True, column_count=1))
        
    def doesTopicExist(self, topic):
        recordedTopics = self.metadata.get_range()
        for topicName in recordedTopics:
            if topicName[0] == topic:
                return True
        return False


                                                                                                                 
