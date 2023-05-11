#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaProducer, KafkaConsumer
from json import loads
import json
import sys

### Setting up the Python consumer
bootstrap_servers = ['localhost:9092']
topicName = 'test.topic.raw'
consumer = KafkaConsumer (topicName, group_id = 'my_group_id4',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))  ## You can also set it as latest


# Required setting for Kafka Producer
                          
topic2 = 'topic.costly_order'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

### Reading the message from consumer
try:
    for message in consumer:
        msg = message.value
        channel= str(msg['Sales Channel'])
        topic2 = 'test.topic.'
        print(channel)
        topic2 += channel
        print(topic2)
        ack = producer.send(topic2,json.dumps(msg).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)
        
except KeyboardInterrupt:
    sys.exit()



# In[ ]:




