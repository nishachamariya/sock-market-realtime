import boto3
import json

s3 = boto3.client('s3', aws_access_key_id='AKxxxx',
                  aws_secret_access_key='xxxx',
                  region_name='ap-south-1')

# S3 bucket
s3_bucket = 'stock-market-nisha'
message = {
  "Date": "2024-02-26",
  "Open": "2987.100098",
  "High": "2989.050049",
  "Low": "2965.000000",
  "Close": "2974.649902",
  "Adj Close": "2974.649902",
  "Volume": "3756553"
}

object_key = "reliance_data.json"

# Upload the message to S3
s3.put_object(Body=json.dumps(message), Bucket=s3_bucket, Key=object_key)
print(f"Message uploaded to S3 with object key: {object_key}")


