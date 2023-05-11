#!/usr/bin/env python
# coding: utf-8

# In[2]:


from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from json import loads
import json
import sys

# Python3 code to remove whitespace 
def remove(string): 
    return string.replace(" ", "") 
      
### Setting up the Python consumer
bootstrap_servers = ['localhost:9092']
topicName = ['test.topic.raw','test.topic.threshold_costs']
consumer = KafkaConsumer (group_id = 'my_group_id3',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8'))) ## You can also set it as latest
consumer.subscribe(['test.topic.raw','test.topic.threshold_costs'])


# Required setting for Kafka Producer
                          
topic2 = 'test.topic.'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

### Reading the message from consumer
try:
    for message in consumer:
        msg = message.value
        country = str(msg['Country'])
        topic2 = 'test.topic.'
        print(remove(country))
        topic2 += remove(country)
        print(topic2)
        ack = producer.send(topic2,json.dumps(msg).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)
        
except KeyboardInterrupt:
    sys.exit()




# In[ ]:





# In[ ]:




