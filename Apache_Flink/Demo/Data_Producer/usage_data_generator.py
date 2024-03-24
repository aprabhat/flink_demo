import datetime
import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer

fake = Faker()

# Define arrays for tenant names, applications, and units
tenant_names = ["tenant1", "tenant2", "tenant3", "tenant4", "tenant5"]  # Add more tenant names if needed
applications = ["datalake", "asset_management"]  # Add more applications if needed
units = ["data_read", "data_write", "number_of_objects", "read_count", "write_count", "object_size", "total_assets"]


def generate_user_event():
    tenant_id = random.choice(tenant_names)
    user_id = fake.email()
    user_type = user_id.split('@')[0]
    resources = [
        {
            "application": random.choice(applications),  # Random application
            "alias": "iotfile",
            "usages": [
                {
                    "value": str(random.randint(1, 100)),  # Sample value
                    "unit": random.choice(units),  # Random unit
                    "datetime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            ]
        }
    ]

    return {
        "transactionId": fake.uuid4(),
        "users": [
            {
                "tenantId": tenant_id,
                "userId": user_id,
                "userType": user_type,
                "resources": resources
            }
        ]
    }


def delivery_report(error, msg):
    if error is not None:
        print(f'Message delivery failed: {error}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition}]')


def main():
    topic = 'usage_events'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.datetime.now()
    while (datetime.datetime.now() - curr_time).seconds < 300:
        try:
            user_event = generate_user_event()
            producer.produce(topic,
                             key=user_event['transactionId'],
                             value=json.dumps(user_event),
                             on_delivery=delivery_report
                             )
            producer.poll(0)

            # wait for 2 seconds before sending the next message
            time.sleep(2)
        except BufferError:
            print('Buffer full! Waiting...')
            time.sleep(1)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
