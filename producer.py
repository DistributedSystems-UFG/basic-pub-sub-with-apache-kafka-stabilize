from kafka import KafkaProducer
from const import *
import kafka

admin_client = kafka.KafkaAdminClient(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

try:
    topics = admin_client.list_topics()

except:
    print ('Usage: python3 producer <topic_name>')
    exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

for topic in topics:
    msg = 'Hello! my ' + topic + ' topic';
    print('Sending message: ' + msg);
    producer.send(topic, value=msg.encode());

producer.flush()
