import boto3
import athena
import athenaquery
import os
import estimates
import getmetrics
import dynamodb
import re


def check_status(status, error_message):
    if status == 'SUCCEEDED' or (isinstance(status, dict) and status.get('status') == 'SUCCEEDED'):
        return True
    else: 
        raise Exception(error_message)
    

def get_metrics(params,regions):
    print("fetching CW Metrics for " + params['dynamodb_tablename'] +  " Dynamo Tables\n")
    status = getmetrics.get_metrics(params,regions)
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

def get_dynamodb_scaling_info(params):
    get_scaling_info = dynamodb.get_dynamodb_scaling_info(params)
    check_status(get_scaling_info, get_scaling_info)
    print('getting DynamoDB scaling info: ' + get_scaling_info['status'])
    return get_scaling_info

def autoscaling_recommendation(params):
    autoscaling_status = athenaquery.create_dynamo_autoscaling_recommendation(params)
    check_status(autoscaling_status, autoscaling_status)
    print('creating autoscaling recommendation: ' + autoscaling_status)
    return autoscaling_status



def get_params(event):
    params = {}
    keys = ['action', 'accountid', 'dynamodb_tablename', 'dynamodb_read_utilization', 'dynamodb_write_utilization', 'dynamodb_minimum_units', 'number_of_days_look_back', 'cloudwatch_metric_end_datatime','regions']
    for key in keys:
        if key in event:
            value = event[key]
            if key == 'action':
                valid_actions = ['create', 'append']
                if not isinstance(value, str) or not value.strip() or value not in valid_actions:
                    raise ValueError(f"Invalid action '{key}'. action must be a non-empty string and one of {valid_actions}")
            elif key == 'accountid':
                if not isinstance(value, str) or not value.isalnum():
                    raise ValueError(f"Invalid accountid '{key}'. accountid must be a non-empty string")
            elif key == 'dynamodb_tablename':
                if not isinstance(value, str) or not value.isalnum():
                    raise ValueError(f"Invalid dynamodb_tablename '{key}'. dynamodb_tablename must be a non-empty string")
            elif key == 'dynamodb_read_utilization':
                if not isinstance(value, int) or value < 0 or value > 100:
                    raise ValueError(f"Invalid dynamodb_read_utilization '{key}'. dynamodb_read_utilization must be an integer between 0 and 100")
            elif key == 'dynamodb_write_utilization':
                if not isinstance(value, int) or value < 0 or value > 100:
                    raise ValueError(f"Invalid dynamodb_write_utilization '{key}'. dynamodb_write_utilization must be an integer between 0 and 100")
            elif key == 'dynamodb_minimum_units':
                if not isinstance(value, int) or value < 0 :
                    raise ValueError(f"Invalid dynamodb_minimum_units '{key}'. dynamodb_minimum_units must be a positive integer")
            elif key == 'number_of_days_look_back':
                if not isinstance(value, int) or value < 0 :
                    raise ValueError(f"Invalid number_of_days_look_back '{key}'. number_of_days_look_back must be a positive integer")
            elif key == 'cloudwatch_metric_end_datatime':
                if not isinstance(value, str) or not re.match(r'20[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', value):
                     raise ValueError("cloudwatch_metric_end_datatime must be a valid string in format 'YYYY-MM-DD HH:MM:SS'")
            elif key == 'regions':
                if not isinstance(value, (list, tuple)):
                    raise ValueError(f"{key} must be a list or tuple")
                for region in value:
                    if not isinstance(region, str) or not region.strip():
                        raise ValueError(f"each element of {key} must be a valid alphanumeric string")
            params[key] = value
    return params
    
def lambda_handler(event,context):

    params = get_params(event)
    params['athena_tablename'] = os.environ['ATHENA_TABLENAME']
    params['athena_database'] = os.environ['ATHENA_DATABASE']
    params['athena_bucket'] = os.environ['ATHENA_BUCKET']
    params['athena_bucket_prefix'] = os.environ['ATHENA_PREFIX']

    try:
        dynamodb_table_info = get_dynamodb_scaling_info(params)
    except Exception as e:
        print(f"Error in getting_dynamnodb_scaling_info: {e}")
        return { 'Message': str(e), 'StatusCode': 500 }
    
    try:
        get_metrics(params,dynamodb_table_info['regions'])
    except Exception as e:
        print(f"Error in get_metrics: {e}")
        return { 'Message': str(e), 'StatusCode': 500 }
            
    try:
        if params['action'] == 'create':
            cost_estimate(params)
            recommendation(params)
            autoscaling_recommendation(params)
    except Exception as e:
        print(f"Error in creating Athena Views : {e}")
        return { 'Message': str(e), 'StatusCode': 500 }
    
    return { 'Message': 'Success', 'StatusCode': 200 }