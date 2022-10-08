import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import json
import argparse
import datetime
import csv




FILE_PATH = "C:/Users/11ana/OneDrive/Desktop/KAFKA/restaurant_orders.csv"


API_KEY = 'DHA3ILPTBNNCOWAH'
API_SECRET_KEY = '5kpHGMZnVUxB2fSKXV9PEEDoNsR47MMVOQ0IgQ7lMOMaePepv6hMLSp8gmROe8sc'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'

SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'

ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'JNMVA6Z75X3RL4WD'
SCHEMA_REGISTRY_API_SECRET = '45K/bJOrZgMPAfdz5lEHhhtmowPpUrfQNeKBxG94NZfW2KLuoj1j+PEqeyJAvWaj'



def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info': f'{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}'

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):


    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    my_schema = schema_registry_client.get_schema(schema_id=100003).schema_str

    json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})     #or earliest, latest

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter=0
    while True:
        try:
           
            msg = consumer.poll(1.0)
            if msg is None:
                continue


            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order is not None:
                    counter+=1
                    print(datetime.datetime.now())
                    print("User record {}: order: {}\n"
                          .format(msg.key(), order))

                  
                    

                    print('Total messages fetched till now:', counter)
                    
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")