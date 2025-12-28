import json
import random
from faker import Faker
import os
from datetime import timedelta, datetime
import boto3

sqs_client = boto3.client('sqs')
QUEUE_URL = os.environ['SQS_QUEUE_URL']  # replace with your SQS Queue URL

def generate_sales_order():
    fake = Faker()
    start_date = fake.date_between_dates(date_start=datetime.now(), date_end=datetime.now() + timedelta(days=5))
    end_date = start_date + timedelta(days=random.randint(1, 5))

    booking = {
        "bookingId": str(fake.uuid4()),
        "userId": str(fake.uuid4()),
        "propertyId": str(fake.uuid4()),
        "location": f"{fake.city()}, {fake.country()}",
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "price": round(random.uniform(50, 500), 2)
    }

    return booking


def lambda_handler(event, context):
    i=0
    while(i<100):
        sales_order = generate_sales_order()
        print(sales_order)
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(sales_order)
        )
        i += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Sales order data published to SQS!')
    }
