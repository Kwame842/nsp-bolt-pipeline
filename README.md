# NSP Bolt Ride – Real-Time Trip Processing Pipeline

This project implements a scalable, serverless, and event-driven data processing pipeline for real-time tracking and analysis of ride-hailing trip events. Built on AWS, it processes trip start and end events, validates and deduplicates data, detects trip completions, and computes daily KPIs such as total fares, average fare, and trip count. Results are stored in S3 for downstream analytics. The system leverages AWS Step Functions to orchestrate the KPI calculation process, ensuring reliability, modularity, cloud-native scalability, and fault tolerance using AWS services like Kinesis, Lambda, DynamoDB, Glue, S3, Step Functions, and CloudWatch.

---

## Project Overview

### Goals

- Ingest trip start and end events in real-time via Amazon Kinesis.
- Validate and deduplicate events before storing in DynamoDB.
- Merge trip start and end events to detect completed trips.
- Compute daily KPIs (e.g., total fare, average fare, trip count, min/max fare) using a scheduled AWS Glue job orchestrated by Step Functions.
- Store aggregated results in S3 for reporting or dashboarding.
- Ensure fault tolerance with dead-letter queues (SQS) and monitoring (CloudWatch).

### Key KPIs Tracked

- Total number of completed trips per day.
- Total daily revenue (sum of fares).
- Average trip fare.
- Minimum and maximum trip fares.

---

## Architecture Overview

| Component                | AWS Service         | Purpose                                                                  |
| ------------------------ | ------------------- | ------------------------------------------------------------------------ |
| `trip-start-stream`      | Amazon Kinesis      | Ingests trip start events from simulators or external sources.           |
| `trip-end-stream`        | Amazon Kinesis      | Ingests trip end events from simulators or external sources.             |
| `TripStartProcessor`     | AWS Lambda          | Validates and stores trip start events in DynamoDB.                      |
| `TripEndProcessor`       | AWS Lambda          | Validates trip end events, checks for trip completion, updates DynamoDB. |
| `TripData`               | Amazon DynamoDB     | Stores trip records with trip_id as the partition key.                   |
| `daily_kpi_aggregator`   | AWS Glue            | Aggregates completed trips daily and writes KPIs to S3.                  |
| `KPIOrchestrator`        | AWS Step Functions  | Schedules and triggers the Glue job for daily KPI computation.           |
| `nsp-kpi-results-bucket` | Amazon S3           | Stores KPI results as timestamped JSON files.                            |
| `TripDLQ`                | Amazon SQS (DLQ)    | Captures failed events from Lambda for retry or debugging.               |
| `CloudWatch Logs`        | Amazon CloudWatch   | Logs Lambda, Glue, and Step Functions executions; triggers alerts.       |

---

## Data Flow

1. **Event Ingestion**: Trip start/end events are streamed to separate Kinesis streams.
2. **Event Processing**: Lambda functions validate and store data in DynamoDB.
3. **Trip Completion**: TripEndProcessor detects completed trips.
4. **KPI Aggregation**: Step Functions triggers the Glue job daily to compute KPIs and write JSON to S3.
5. **Error Handling**: DLQ captures failed Lambda events for review.

---

## Project Structure

```bash
nsp-bolt-trip-pipeline/
├── .github/
│   └── workflows/              # CI/CD for Github Actions
│       └── main.yml
├── diagram/                    # Architecture Diagram
│   └── Architecture7.png
├── lambdas/                    # Lambda functions
│   ├── TripStartProcessor.py
│   ├── TripEndProcessor.py
├── glue_jobs/                  # Glue ETL jobs
│   └── KPI-Aggregator.py
├── step_functions/             # Step Functions state machine
│   └── KPI-Orchestrator.json
├── imgs/                       # Screenshots
│   └── screenshots.png
├── simulator/                  # Simulator to push events
│   └── simulator.py
├── tests/                      # Optional test suite
├── README.md                   # Project documentation
└── requirements.txt            # Dev dependencies
```

---

## Deployment Instructions

### 1. Provision Infrastructure

```bash
aws cloudformation deploy --template-file infrastructure/template.yaml --stack-name nsp-bolt-trip-pipeline --capabilities CAPABILITY_NAMED_IAM
```

### 2. Deploy Lambda Functions

```bash
cd lambdas/trip_start_processor/
pip install -r requirements.txt -t .
zip -r9 ../trip_start_processor.zip .
# Upload zip to Lambda console or via CLI
```

### 3. Configure Event Triggers

```bash
aws lambda create-event-source-mapping --function-name TripStartProcessor --event-source-arn arn:aws:kinesis:region:account-id:stream/trip-start-stream --batch-size 100 --starting-position LATEST
```

### 4. Deploy Step Functions State Machine

```bash
aws stepfunctions create-state-machine --name KPIOrchestrator --definition file://step_functions/KPI-Orchestrator.json --role-arn arn:aws:iam::account-id:role/StepFunctionsExecutionRole
```

### 5. Schedule Step Functions Execution

- Create a CloudWatch Events Rule to trigger the Step Functions state machine daily at 00:00 UTC.
```bash
aws events put-rule --name DailyKPIOrchestration --schedule-expression "cron(0 0 * * ? *)"
aws events put-targets --rule DailyKPIOrchestration --targets "Id=1,Arn=arn:aws:states:region:account-id:stateMachine:KPIOrchestrator"
```

### 6. Test Pipeline

```bash
cd simulator/
python simulate.py
```

---

## Sample Output

```json
{
  "date": "2025-07-11",
  "total_fare": 18123.5,
  "count_trips": 254,
  "average_fare": 71.35,
  "min_fare": 12.5,
  "max_fare": 103.75
}
```

---

## Requirements

### Root

```txt
boto3
botocore
jsonschema
pytest
moto[dynamodb2]
```

### Lambda Functions

```txt
boto3
jsonschema
```

### Glue

```txt
boto3
decimal
```

---

## Security & Best Practices

- Enable encryption on S3 and DynamoDB.
- Use least privilege IAM policies for Lambda, Glue, and Step Functions.
- Log all Lambda, Glue, and Step Functions activities to CloudWatch.
- Store sensitive values in Secrets Manager or environment variables.

---

## FAQ

**Q: Why use two Kinesis streams?**  
A: Decouples ingestion and improves scaling and debugging.

**Q: How are missing events handled?**  
A: Trip remains incomplete in DynamoDB and excluded from KPI.

**Q: Why use Step Functions for the Glue job?**  
A: Step Functions provides reliable scheduling, error handling, and orchestration for the daily KPI aggregation.

**Q: Is this scalable?**  
A: Yes, with Kinesis shards, Lambda concurrency, DynamoDB autoscaling, and Step Functions orchestration.

---

## Author

Developed by Kwame A. Boateng

## License

MIT License