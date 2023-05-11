#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
import sys
import json
from json import loads
### Setting up the Python consumer
bootstrap_servers = ['localhost:9092']
topicName = 'test.topic.raw'
consumer = KafkaConsumer (topicName, group_id = 'my_group_id8',bootstrap_servers = bootstrap_servers,
    auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))  ## You can also set it as latest


 
# Required setting for Kafka Producer

topic2 = 'test.topic.threshold_costs'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()


### Reading the message from consumer
try:
    for message in consumer:
        print (message.value)
        msg = message.value
        f = float(msg['Total Cost'])
        if f > 304017.56:
            print(msg)
            ack = producer.send(topic2,json.dumps(msg).encode('utf-8'))
            metadata = ack.get()
            print(metadata.topic, metadata.partition)
        
except KeyboardInterrupt:
    sys.exit()
 
   


# In[ ]:




