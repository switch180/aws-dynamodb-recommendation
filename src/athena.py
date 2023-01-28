import boto3
import time
import os


client = boto3.client('athena')

def query_results(session, params):
    
    response_query_execution_id = client.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        },
    )
    response_get_query_details = client.get_query_execution(
        QueryExecutionId = response_query_execution_id['QueryExecutionId']
    )
    status = 'RUNNING'
    while (status != 'SUCCEEDED'):
        response_get_query_details = client.get_query_execution(
        QueryExecutionId = response_query_execution_id['QueryExecutionId']
        )
        status = response_get_query_details['QueryExecution']['Status']['State']
        time.sleep(1)
        if status in ['FAILED' , 'CANCELLED']:
            break
        
    location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

    
    return status,location
