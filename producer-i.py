#!/usr/bin/env python

import sys
import pandas as pd
from datetime import datetime
import time
import random
import numpy as np
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "ITopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    kafka_producer_obj = Producer(config)


    filepath = "/home/humaira/CODE/Raccoon-AI-Engine/datasets/IRIS.csv"
    
    flower_df = pd.read_csv(filepath)
  
    flower_df['order_id'] = np.arange(len(flower_df))

    
    flower_list = flower_df.to_dict(orient="records")
       

    message_list = []
    message = None
    for message in flower_list:
        
        message_fields_value_list = []
               
        message_fields_value_list.append(message["order_id"])
        message_fields_value_list.append(message["sepal_length"])
        message_fields_value_list.append(message["sepal_width"])
        message_fields_value_list.append(message["petal_length"])
        message_fields_value_list.append(message["petal_width"])
        message_fields_value_list.append(message["species"])

        message = ','.join(str(v) for v in message_fields_value_list)
        #print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.produce(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)


    print("Kafka Producer Application Completed. ")
