import boto3
import json
import os
import time

sns = boto3.client('sns', region_name='ap-south-1')
athena = boto3.client('athena', region_name='ap-south-1')

ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'mandi_db')
ATHENA_OUTPUT = os.environ['ATHENA_OUTPUT'] 
SUBSCRIBERS = json.loads(os.environ.get('SUBSCRIBERS', '[]'))


def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    execution_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        return []

    results = athena.get_query_results(QueryExecutionId=execution_id)
    rows = results['ResultSet']['Rows']
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    return [
        dict(zip(headers, [c.get('VarCharValue', '') for c in row['Data']]))
        for row in rows[1:]
    ]


def lambda_handler(event, context):
    alerts_sent = 0

    for farmer in SUBSCRIBERS:
        commodity = farmer['commodity']
        phone = farmer['phone']

        query = f"""
            SELECT commodity, market, state, district, modal_price
            FROM best_markets
            WHERE commodity = '{commodity}'
            AND price_rank = 1
            AND modal_price IS NOT NULL
            LIMIT 1
        """

        results = run_athena_query(query)

        if results and results[0].get('modal_price'):
            r = results[0]
            message = (
                f"Mandi Alert: {r['commodity']} is paying "
                f"Rs {r['modal_price']}/quintal in {r['market']}, "
                f"{r['district']}, {r['state']}. Consider selling there today."
            )
            sns.publish(PhoneNumber=phone, Message=message)
            alerts_sent += 1
        else:
            print(f"No price data available today for {commodity}")

    return {"statusCode": 200, "alerts_sent": alerts_sent}
