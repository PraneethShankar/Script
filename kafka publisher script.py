from kafka import KafkaProducer
import time

KAFKA_SERVER = "3.109.89.122"
#54.69.79.78
#54.70.253.233
#KAFKA_SERVER = "3.109.141.121"
#KAFKA_SERVER = "54.218.254.115"
#KAFKA_SERVER = "54.70.253.233"
KAFKA_PORT = 9092
#topic = "rtuparams"
#topic = "rtualarm"
#topic = "rtunetstatus"
#topic = "devicealarm"
#topic = "rtustatus"
#topic = "devicebinary"
topic = "devicemetrics"
#topic = "rtuerrors"
#topic = "command"
#topic = "sendtrip"
#topic = "billingLoadAccountsBatch"
#topic = "lantratest3"
#topic = "sendtrip"
#topic = "readparam"
#topic = "writeparam"
#topic = "create-trip-prod"
#topic = "dlmsdata"
# topic = "create-trip"

connection_str = KAFKA_SERVER + ":" + str(KAFKA_PORT)
global kafka_producer
kafka_producer = KafkaProducer(bootstrap_servers=connection_str)

#
# Post to a given KAFKA topic
#
def post_to_kafka(topicname, kafka_str):
    print("DEBUG: KAFKA Producer = ", kafka_producer)
    print("DEBUG: To Post to KAFKA = ", kafka_str, " in topic:", topicname)
    future = kafka_producer.send(topicname, str.encode(kafka_str))
    result = future.get(timeout=60)
    print("DEBUG: Posted to KAFKA")

#
# Read from file and post to kafka
#
with open('devmetrics.txt') as fp:
    for line in fp:
        print(line.rstrip())
        post_to_kafka(topic, line.rstrip())
        time.sleep(10)
