import boto3
import csv
import json
import time
import codecs
from io import StringIO
from datetime import datetime

# AWS clients
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

# Configurations
bucket = 'nsp-kpi-results-bucket'
stream_map = {
    'start': 'trip_start_stream',
    'end': 'trip_end_stream'
}
batch_size = 250
sleep_between_batches = 0.25

REQUIRED_FIELDS = {
    'start': ['trip_id', 'pickup_datetime'],
    'end': ['trip_id', 'dropoff_datetime', 'fare_amount']
}

def normalize_record(row, event_type):
    row['event_type'] = event_type
    row['source'] = 'simulator'

    # Normalize datetime
    for field in ['pickup_datetime', 'dropoff_datetime']:
        if field in row and row[field]:
            try:
                try:
                    dt = datetime.strptime(row[field], '%d/%m/%Y %H:%M')
                except ValueError:
                    dt = datetime.strptime(row[field], '%Y-%m-%d %H:%M:%S')
                row[field] = dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                print(f" Invalid {field}: {row[field]} ({e})")

    # Cast numeric fields
    float_fields = ['fare_amount', 'tip_amount', 'trip_distance', 'estimated_fare_amount']
    int_fields = ['passenger_count', 'rate_code', 'trip_type', 'payment_type',
                  'vendor_id', 'pickup_location_id', 'dropoff_location_id']

    for f in float_fields:
        if f in row and row[f]:
            try:
                row[f] = float(row[f])
            except:
                pass

    for f in int_fields:
        if f in row and row[f]:
            try:
                row[f] = int(float(row[f]))
            except:
                pass

    return row

def send_batch_to_kinesis(records, stream_name):
    if not records:
        return
    try:
        response = kinesis.put_records(StreamName=stream_name, Records=records)
        failed = response.get("FailedRecordCount", 0)
        print(f" Sent {len(records)} records to {stream_name} (Failed: {failed})")
    except Exception as e:
        print(f" Failed to send batch to {stream_name}: {e}")

def process_file_from_s3(s3_key, event_type):
    print(f"\n Streaming '{s3_key}' as '{event_type}' events...")
    stream_name = stream_map[event_type]
    try:
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        body = obj['Body']
        reader = csv.DictReader(codecs.getreader('utf-8')(body))

        batch = []
        for row in reader:
            if not all(f in row and row[f].strip() for f in REQUIRED_FIELDS[event_type]):
                print(f" Skipped row with missing fields: {row.get('trip_id', '[no id]')}")
                continue

            record = normalize_record(row, event_type)
            batch.append({
                'Data': json.dumps(record),
                'PartitionKey': record['trip_id']
            })

            if len(batch) >= batch_size:
                send_batch_to_kinesis(batch, stream_name)
                batch = []
                time.sleep(sleep_between_batches)

        if batch:
            send_batch_to_kinesis(batch, stream_name)

    except Exception as e:
        print(f" Error reading {s3_key}: {e}")

# Run simulation
if __name__ == '__main__':
    process_file_from_s3('trip_start.csv', 'start')
    process_file_from_s3('trip_end.csv', 'end')