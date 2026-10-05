from confluent_kafka import *
import random
import time
from datetime import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

producer_config = {
    "bootstrap.servers": os.getenv("FABRIC_BOOTSTRAP_SERVER"),
    "security.protocol": os.getenv("FABRIC_SECURITY_PROTOCOL"),
    "sasl.mechanisms": os.getenv("FABRIC_SASL_MECHANISM"),
    "sasl.username": os.getenv("FABRIC_USERNAME"),
    "sasl.password": os.getenv("FABRIC_PASSWORD")
}

prod = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)
    else:
        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )
        
products = [
    {
        "product_id": 1,
        "product_name": "Laptop",
        "price": 1200.0
    },
    {
        "product_id": 2,
        "product_name": "Phone",
        "price": 800.0
    },
    {
        "product_id": 3,
        "product_name": "Headphones",
        "price": 100.0
    },
    {
        "product_id": 4,
        "product_name": "Keyboard",
        "price": 70.0
    },
    {
        "product_id": 5,
        "product_name": "Mouse",
        "price": 40.0
    }
]

order_id=1
while True:

    product = random.choice(products)

    order = {
        "order_id": order_id,
        "customer_id": random.randint(1, 100),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "quantity": random.randint(1, 5),
        "price": product["price"],
        "order_time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    }
    value = json.dumps(order).encode("utf-8")
    topic = os.getenv("FABRIC_TOPIC")
    prod.produce(
            topic,
            value=value,
            callback=delivery_report)
    
    prod.flush()

    print("Sent:", order)

    order_id += 1

    time.sleep(1)