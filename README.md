# web-analytics-project
Website that sends clickstream data to Kinesis Data Stream (Go).

## Setup

1. Create a Kinesis Data Stream in AWS.

2. Set environment variables:
   ```bash
   export KINESIS_STREAM_NAME=your_stream_name
   export AWS_REGION=us-east-1
   # AWS credentials are picked up from ~/.aws/credentials, env vars, or IAM role
   ```

3. Run:
   ```bash
   go run main.go
   ```

4. Open http://localhost:3000 and click around.

## Event schema

```json
{
  "session_id": "uuid",
  "event_type": "click",
  "element": "BUTTON",
  "element_id": "btn1",
  "x": 120,
  "y": 340,
  "path": "/",
  "timestamp": "2026-05-25T07:45:00.000Z"
}
```

## IAM permissions required

```json
{
  "Effect": "Allow",
  "Action": "kinesis:PutRecord",
  "Resource": "arn:aws:kinesis:<region>:<account-id>:stream/<stream-name>"
}
```
