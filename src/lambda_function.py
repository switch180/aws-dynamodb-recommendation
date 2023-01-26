import boto3
import athena
import athenaquery
import os
import estimates
import getmetrics

def check_status(status, error_message):
    if status not in ['Success', 'SUCCEEDED']:
        raise Exception(error_message)
    return True

def get_metrics(params):
    print("fetching CW Metrics for " + params['dynamodb_tablename'] +  " Dynamo Tables\n")
    status = getmetrics.get_metrics(params)
    check_status(status, status)
    print("CW Get Metrics Job: " + status)
    return status


def cost_estimate(params):
    cost_est_status = athenaquery.create_cost_estimate(params)
    check_status(cost_est_status, cost_est_status)
    print('estimating cost: ' + cost_est_status)
    return cost_est_status

def recommendation(params):
    recommendation_status = athenaquery.create_dynamo_mode_recommendation(params)
    check_status(recommendation_status, recommendation_status)
    print('creating recommendation: ' + recommendation_status)
    return recommendation_status

def reservation(params):
    reserv_status = athenaquery.create_reserved_cost(params)
    check_status(reserv_status, reserv_status)
    print('creating reservation recommendation: ' + reserv_status)
    return reserv_status


def get_params(event):
    params = {}
    keys = ['action', 'accountid', 'dynamodb_tablename', 'dynamodb_read_utilization', 'dynamodb_write_utilization', 'dynamodb_minimum_units', 'number_of_days_look_back', 'cloudwatch_metric_end_datatime']
    for key in keys:
        if key in event:
            params[key] = event[key]
    return params
    
def lambda_handler(event,context):

    params = get_params(event)
    params['athena_tablename'] = os.environ['ATHENA_TABLENAME']
    params['athena_database'] = os.environ['ATHENA_DATABASE']
    params['athena_bucket'] = os.environ['ATHENA_BUCKET']
    params['athena_bucket_prefix'] = os.environ['ATHENA_PREFIX']


    try:
        get_metrics(params)
    except Exception as e:
        print(f"Error in get_metrics: {e}")
        return { 'Message': str(e), 'StatusCode': 500 }
        
    try:
        if params['action'] == 'create':
            cost_estimate(params)
            recommendation(params)
            reservation(params)
    except Exception as e:
        print(f"Error in create action: {e}")
        return { 'Message': str(e), 'StatusCode': 500 }
    
    return { 'Message': 'Success', 'StatusCode': 200 }
