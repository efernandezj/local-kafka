import json
import random
import uuid
# from datetime import datetime
from datetime import datetime, UTC

from confluent_kafka import Producer


def delivery_report(err, msg):
    if err:
        print(f"Error: {err}")
    else:
        print(
            f"Delivered "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode('utf-8')}"
        )


producer = Producer(
    {
        "bootstrap.servers": "localhost:9092"
    }
)

for _ in range(50):

    order_id = random.randint(1001, 2001)

    key = f"order-{order_id}"

    payload = {
        "orderId": order_id,
        "customerId": f"CUST-{random.randint(1, 500)}",
        "status": random.choice(
            [
                "CREATED",
                "PAID",
                "SHIPPED",
                "DELIVERED"
            ]
        ),
        "currency": "USD",
        "amount": round(random.uniform(20, 500), 2),
        "createdAt": datetime.now(UTC).isoformat()
        # "createdAt": datetime.utcnow().isoformat()
    }

    headers = {
        "eventType": "OrderUpdated",
        "source": "python-order-producer",
        "version": "v1",
        "correlationId": str(uuid.uuid4()),
        "environment": "local",
        "contentType": "application/json"
    }

    producer.produce(
        topic="test",
        key=key,
        value=json.dumps(payload),
        headers=headers,
        callback=delivery_report
    )

producer.flush()