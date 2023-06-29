

from confluent_kafka import Consumer, KafkaError

c = Consumer({
    'bootstrap.servers': 'localhost:9091',
    'group.id': 'counting-group',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['light_bulb'])
while True:
    msg = c.poll(0.1)
    if msg is None:
        continue
    elif not msg.error():
        print(msg.value())
    elif msg.error().code() == KafkaError._PARTITION_EOF:
        print("End of partition reached")
    else:
        print("Error")