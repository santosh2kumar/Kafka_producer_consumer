#!/usr/bin/env python
# coding: utf-8

# In[7]:


from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
import json
from json import loads
from csv import DictReader

# Required setting for Kafka Producer
bootstrap_servers = ['localhost:9092']
topicname = 'test.topic.raw'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()


# iterate over each line as a ordered dictionary and print only few column by column name
with open('SalesRecords.csv','r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        ack = producer.send(topicname, json.dumps(row).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)



# In[ ]:





# In[ ]:




