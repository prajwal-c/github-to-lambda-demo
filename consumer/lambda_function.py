import boto3
from io import StringIO
from datetime import datetime
import time
import os
import csv
import json

def lambda_handler(event, context):
    print("Starting SQS Batch Process")
    
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    unix_ts = int(time.time())

    # Specify your SQS queue URL
    queue_url = os.environ['SQS_QUEUE_URL']
    bucket = os.environ['bucket']

    # Create SQS client and S3 client
    sqs = boto3.client('sqs')
    s3 = boto3.client("s3")
    bucket = os.environ['bucket']
    file_name = "bookings"
    s3_key = f"airbnb_data/{date_str}/{file_name}_{unix_ts}.csv"

    # Receive messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # Adjust based on your preference
        WaitTimeSeconds=20       # Use long polling
    )

    messages = response.get('Messages', [])
    print("Total messages received in the batch : ",len(messages))
    valid_records = 0

    if not messages:
        return {"statusCode": 200, "body": "No messages to process"}
    
    # Create CSV in memory
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["booking_id", "user_id", "property_id", "location", "start_date", "end_date", "price"])

    for message in messages:

        # Process message
        print("Processing message: ", message['Body'])
        msg_body = json.loads(message["Body"])
        try:
            start_date = datetime.strptime(msg_body.get("startDate"), "%Y-%m-%d")
            end_date = datetime.strptime(msg_body.get("endDate"), "%Y-%m-%d")
            date_diff = (end_date - start_date).days
        except Exception as e:
            print("Date parsing error:", e)
            continue
        
        if date_diff > 1:
            writer.writerow([
                msg_body.get("bookingId"),
                msg_body.get("userId"),
                msg_body.get("propertyId"),
                msg_body.get("location"),
                msg_body.get("startDate"),
                msg_body.get("endDate"),
                msg_body.get("price")
            ])
            valid_records += 1

        # Delete message from the queue
        receipt_handle = message['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Message deleted from the queue")

    if valid_records > 0:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        print(f"{valid_records} records written to S3")
    else:
        print("No records met the date difference condition")
        
    print("Ending SQS Batch Process")

    return {
        'statusCode': 200,
        'body': f'{len(messages)} booking records received and {valid_records} records saved to S3'
    }