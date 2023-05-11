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
consumer = KafkaConsumer (topicName, group_id = 'my_group_id1',bootstrap_servers = bootstrap_servers,
    auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))  ## You can also set it as latest
    
### Reading the message from consumer
try:
    for message in consumer:
        print (message.value)
except KeyboardInterrupt:
    sys.exit()
    


# In[ ]:




