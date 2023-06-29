from confluent_kafka import Producer
from faker import Faker
import time

fake = Faker()

p = Producer({'bootstrap.servers': 'localhost:9091'})
print("Generating fake data...")
for _ in range(10):
    key = fake.uuid4()
    value = fake.sentence()
    p.produce('light_bulb', key=key, value=value)
    print(f"Produced: key={key}, value={value}")
    time.sleep(2)
    
p.flush(30)
print("Data generation completed.")
