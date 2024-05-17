#!/usr/bin/env python

import sys
import pandas as pd
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json

if __name__ == '__main__':
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
    p = Producer(config)
    def delivery_report(err, msg):
    	""" Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    	if err is not None:
        	print('Message delivery failed: {}'.format(err))
    	else:
        	print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        	
    
    data_source = pd.read_csv("/home/humaira/CODE/Raccoon-AI-Engine/datasets/housedata.csv")
    #data_dict = data_source.to_dict()
    #for data in data_source:
    page_size = 110 
    partition = len(data_source)/page_size
    for i in range(int(partition)):
    
    	sample = data_source.iloc[i*page_size:i*page_size + page_size].to_dict()
    	data = json.dumps(sample)
    	p.poll(0)
    	p.produce('mytopic', data.encode('utf-8'), callback=delivery_report)
    	if i == int(partition)-1 and partition - int(partition)>0:
    	
    		print('ran')
    		sample = data_source.iloc[(i+1)*page_size:].to_dict()
    		data = json.dumps(sample)
    		p.poll(0)
    		p.produce('mytopic', data.encode('utf-8'), callback=delivery_report)
    	# Trigger any available delivery report callbacks from previous produce() calls
    	#data_dict = data_source[data].to_dict()
    	#data = json.dumps(data_dict)
    	# Asynchronously produce a message. The delivery report callback will
    	# be triggered from the call to poll() above, or flush() below, when the
    	# message has been successfully delivered or failed permanently.

	# Wait for any outstanding messages to be delivered and delivery report
	# callbacks to be triggered.
    p.flush()
