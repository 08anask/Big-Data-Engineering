import pandas as pd
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer ,SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from typing import List

FILE_PATH = "C:/Users/11ana/OneDrive/Desktop/KAFKA/restaurant_orders.csv"
columns=['order_number','order_date','item_name','quantity','product_price','total_products']

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


def get_order_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,:]
    
    orders:List[Order]=[]    
    for data in df.values:
        order=Order(dict(zip(columns,data)))
        
        orders.append(order)
        yield order

def order_to_dict(order:Order, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """


    return order.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    

    
    topic = 'restaurent-take-away-data'
    my_schema = schema_registry_client.get_latest_version(topic+'-value').schema.schema_str  
    
    string_serializer = StringSerializer('utf_8')
    
    json_serializer = JSONSerializer(my_schema, schema_registry_client, order_to_dict)    

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    
    producer.poll(0.0)
    try:
        i=0
        for order in get_order_instance(file_path=FILE_PATH):

            print(order)
            producer.produce(topic=topic,
              
                            key=string_serializer(str(uuid4()), order_to_dict),
                            value=json_serializer(order, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            i+=1

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    #flush the buffer memory
    producer.flush()

main("restaurent-take-away-data")
    
    
    
    
    
    
    
