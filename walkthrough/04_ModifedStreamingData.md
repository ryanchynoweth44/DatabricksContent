# Send Taxi Data to EventHub
In this walkthrough we will create a new send data script to handle our Taxi machine learning solution. A copy of the script we implement can be found [here](../../code/SendTaxiData.py).

**Please make sure you are no longer sending data to your event hub using the "SendData" Python notebook**

## Create a new Send Data for Taxi Tip Predictions
1. In Azure Databricks create a python notebook called "SendTaxiData". We will be generating and sending data to our event hub to make streaming machine learning predictions using the model we just trained.  

1. Import your required libraries by pasting the following code into your "SendTaxiData" notebook. Run the cell by attaching the notebook to your cluster and "CTRL+Enter". 
    ```python
    # import the required resources
    import sys
    import logging
    import datetime
    import time
    import os
    import random

    from azure.eventhub import EventHubClient, Sender, EventData
    ```

1. Create data generation methods to generate random data within the bounds we set. This is not perfect but will suffice for this walkthrough. 
    ```python
    def get_total_amount():
    return str((random.random()*10)+random.randint(1,70))

    def get_distance():
    return str(random.random() + random.randint(1,7))

    def get_passenger():
    return str(random.randint(1,6))

    def get_trip_time():
    return str(random.randint(1,1000))
    ```

1. Set up your connection strings using the following code. Please provide the shared access policy with the "Send" credentials, and the event hub name space.       
    ```python
    logger = logging.getLogger("azure")

    # For example: "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
    ADDRESS = ""

    # SAS policy and key are not required if they are encoded in the URL
    USER = ""
    KEY = ""
    ```

1. Now it is time to send data to your Event Hub! The following code sends random taxi data to the event hub.  
    ```python 
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
                    json_string = "{'passenger_count': '%s', 'trip_time_in_secs': '%s', 'trip_distance': '%s', 'total_amount': '%s'}" % (get_passenger(), get_trip_time(), get_distance(), get_total_amount()  ) 
                    print("Sending message: {}".format(json_string))
                    sender.send(EventData(json_string))
                    time.sleep(7)
            except:
                raise
            finally:
                end_time = time.time()
                client.stop()
                run_time = end_time - start_time
                logger.info("Runtime: {} seconds".format(run_time))

        except KeyboardInterrupt:
            pass
    ```

1. You have successfully create a notebook that sends taxi data to our event hub. It is time to make our streaming predictions!