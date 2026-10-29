import boto3
import json
import urllib.request
import urllib.parse

kinesis = boto3.client('kinesis', region_name='ap-south-1')
secrets = boto3.client('secretsmanager', region_name='ap-south-1')

API_BASE = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
STREAM_NAME = "mandi-price-stream"
SECRET_NAME = "datagovin-key" 


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def lambda_handler(event, context):
    secret_value = secrets.get_secret_value(SecretId=SECRET_NAME)
    api_key = json.loads(secret_value['SecretString'])['key']

    params = {"api-key": api_key, "format": "json", "limit": "1000"}
    url = API_BASE + "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())

    records = data.get("records", [])
    sent = 0

    for batch in chunks(records, 500):
        kinesis_records = [
            {
                "Data": json.dumps({
                    "record_date": r.get("arrival_date"),
                    "state": r.get("state"),
                    "district": r.get("district"),
                    "market": r.get("market"),
                    "commodity": r.get("commodity"),
                    "variety": r.get("variety"),
                    "grade": r.get("grade"),
                    "min_price": r.get("Min X0020 Price"),
                    "max_price": r.get("Max X0020 Price"),
                    "modal_price": r.get("Modal X0020 Price"),
                }),
                "PartitionKey": f"{r.get('state')}-{r.get('commodity')}"
            }
            for r in batch
        ]
        kinesis.put_records(Records=kinesis_records, StreamName=STREAM_NAME)
        sent += len(batch)

    return {"statusCode": 200, "records_sent": sent}
