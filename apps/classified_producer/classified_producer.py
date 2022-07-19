#!/usr/bin/env python3

import time
import json
import random
import string

from kafka import KafkaProducer

def random_classified_id():
    return random.randint(1000, 5000)

def random_user_id():
    return random.randint(1, 10000)

def random_url():
    return f"www.pandonline-classified-ads.com/{get_random_string()}"

def get_random_string():
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(10))

def get_json_data():
    data = {}

    data["classifiedId"] = random_classified_id()
    data["userId"] = random_user_id()
    data["url"] = random_url()

    return json.dumps(data) 

def main():
    producer = KafkaProducer(bootstrap_servers=['redpanda-1:9092'])

    for _ in range(20000):
        json_data = get_json_data()
        producer.send('classifieds', bytes(f'{json_data}','UTF-8'))
        print(f"Classified Ad data is sent: {json_data}")
        time.sleep(5)


if __name__ == "__main__":
    main()