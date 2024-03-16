import datetime
import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer

from datetime import datetime

fake = Faker()


def generate_sales_transactions():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        "productCategory": random.choice(['electronics', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        "productPrice": round(random.uniform(10, 1000), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice(['apple', 'samsung', 'mi', 'boat', 'sony', 'US Polo', 'tata']),
        "currency": random.choice(['INR']),
        "customerId": user['username'],
        "transactionDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit+card', 'online_transfer', 'cash+on_delivery'])
    }


def delivery_report(error, msg):
    if error is not None:
        print(f'Message delivery failed: {error}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition}]')


def main():
    topic = 'financial_transaction'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()
    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_sales_transactions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']
            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report
                             )
            producer.poll(0)

            # wait for 5 seconds before sending the next message
            time.sleep(5)
        except BufferError:
            print('Buffer full! Waiting...')
            time.sleep(1)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
