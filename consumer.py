from kafka import KafkaConsumer
from const import *
import kafka

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
admin_client = kafka.KafkaAdminClient(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

try:
  topics = admin_client.list_topics();

except:
  print ('Usage: python3 consumer <topic_name>')
  exit(1)
  
consumer.subscribe(topics)
for msg in consumer:
    print (msg.value.decode())
