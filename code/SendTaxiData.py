# Databricks notebook source
import sys
import logging
import datetime
import time
import os
import random

from azure.eventhub import EventHubClient, Sender, EventData

# create data generators
def get_total_amount():
  return str(((random.random()*10)+random.randint(1,70))/3)

def get_distance():
  return str((random.random() + random.randint(1,7))/2)

def get_passenger():
  return str(random.randint(1,6))

def get_trip_time():
  return str(random.randint(200,1000))



logger = logging.getLogger("azure")

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:
ADDRESS = "amqps://<mynamespace>.servicebus.windows.net/myeventhub"

# SAS policy and key are not required if they are encoded in the URL
USER = ""
KEY = ""


# send generated data
while(True):
  try:
      if not ADDRESS:
          raise ValueError("No EventHubs URL supplied.")

      # Create Event Hubs client
      client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
      sender = client.add_sender(partition="0")
      client.run()
      try:
          start_time = time.time()
          for i in range(20):
            json_string = "{'passenger_count': '%s', 'trip_time_in_secs': '%s', 'trip_distance': '%s', 'total_amount': '%s', 'created_datetime': '%s'}" % (get_passenger(), get_trip_time(), get_distance(), get_total_amount(), str(datetime.datetime.now())) 
            print("Sending message: {}".format(json_string))
            sender.send(EventData(json_string))
            time.sleep(1)
      except:
          raise
      finally:
          end_time = time.time()
          client.stop()
          run_time = end_time - start_time
          logger.info("Runtime: {} seconds".format(run_time))

  except KeyboardInterrupt:
      pass
  




