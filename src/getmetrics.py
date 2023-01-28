import boto3
import datetime
from pytz import timezone
import pandas as pd
from datetime import datetime, timedelta
import awswrangler as wr
import concurrent.futures
from queue import Queue
import estimates






#list metrics
def list_metrics(tablename: str, region: str) -> list:
    # Create a client for the AWS CloudWatch service using the specified region
    cw = boto3.client('cloudwatch', region_name=region)

    # Create an empty list to store the metrics
    metrics_list = []

    # Create a paginator for the "list_metrics" operation
    paginator = cw.get_paginator('list_metrics')

    # Set the operation parameters based on the provided tablename
    if tablename == 'all':
        operation_parameters = {'Namespace': 'AWS/DynamoDB'}
    else:
        operation_parameters = {'Namespace': 'AWS/DynamoDB',
                                'Dimensions': [{'Name': 'TableName','Value': tablename}]}

    # Iterate through the paginated responses, appending each response's metrics to the list
    for response in paginator.paginate(**operation_parameters):
        metrics_list.extend(response['Metrics'])

    # Return the list of metrics
    return metrics_list
      


def process_results(metr_list,metric,accountid,metric_result_queue,estimate_result_queue,readutilization,writeutilization,region):
    
    metrics_result = []
    for result in metr_list['MetricDataResults']:
        
        try:
            name = str(metric[0]['Value']) + ":" + str(metric[1]['Value'])
        except: 
            name = str(metric[0]['Value'])
        metric_list = list(zip (result['Timestamps'],result['Values']))
        tmdf = pd.DataFrame(metric_list,columns = ['timestamp','unit'])
        tmdf['unit'] =  tmdf['unit'].astype(float)
        tmdf['timestamp'] = pd.to_datetime(tmdf['timestamp'], unit='ms')
        tmdf['name'] =  name
        tmdf['accountid'] =  accountid
        tmdf['region'] =  region
        tmdf['metric_name'] = result['Label']
        tmdf = tmdf[['metric_name','accountid','region','timestamp','name','unit']]
        metrics_result.append(tmdf)
        metric_result_queue.put(tmdf)
    metrics_result = pd.concat(metrics_result)
    estimate_units = estimates.estimate(metrics_result,readutilization,writeutilization)
    
    estimate_result_queue.put(estimate_units)
    
        

def get_table_metrics(metrics, starttime, endtime, consumed_period, provisioned_period, accountid,readutilization,writeutilization,region):
    cw = boto3.client('cloudwatch',region_name=region)
    metric_result_queue = Queue()
    estimate_result_queue = Queue()
    metr_list = []
    #This will ensure that only 10 threads are running at a time to avoid overloading the system
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for metric in metrics:
            # Retriving provisionedCapacityUnits
            if metric['MetricName'] == 'ProvisionedWriteCapacityUnits':
                future = executor.submit(cw.get_metric_data, MetricDataQueries=[ 
                    {
                    'Id' : 'provisioned_rcu',
                    'MetricStat': {
                        'Metric' : {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ProvisionedReadCapacityUnits',
                            'Dimensions': metric['Dimensions']
                            },
                        'Period': provisioned_period,
                        'Stat': 'Average'
                        },
                        },
                    {
                    'Id' : 'provisioned_wcu',
                    'MetricStat': {
                        'Metric' : {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ProvisionedWriteCapacityUnits',
                            'Dimensions': metric['Dimensions']
                            },
                        'Period': provisioned_period,
                        'Stat': 'Average'
                        }
                        }
                ], StartTime= starttime, EndTime= endtime)
                future.add_done_callback(lambda f,metric=metric: process_results(f.result(), metric['Dimensions'], accountid, metric_result_queue,estimate_result_queue,readutilization,writeutilization,region))
                metr_list.append(future)
            # Retriving ConsumedCapacityUnits
            elif metric['MetricName'] == 'ConsumedReadCapacityUnits':
                
                future = (executor.submit(cw.get_metric_data, MetricDataQueries=[ 
                    {
                    'Id' : 'consumed_rcu',
                    'MetricStat': {
                        'Metric' : {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ConsumedReadCapacityUnits',
                            'Dimensions': metric['Dimensions']
                            },
                        'Period': consumed_period,
                        'Stat': 'Sum'
                        },
                        },
                        {
                    'Id' : 'consumed_wcu',
                    'MetricStat': {
                        'Metric' : {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ConsumedWriteCapacityUnits',
                            'Dimensions': metric['Dimensions']
                            },
                        'Period': consumed_period,
                        'Stat': 'Sum'
                        }
                        }
                ], StartTime= starttime, EndTime= endtime))
                future.add_done_callback(lambda f,metric=metric: process_results(f.result(), metric['Dimensions'], accountid, metric_result_queue,estimate_result_queue,readutilization,writeutilization,region))
                metr_list.append(future)
        
    # Wait for all of the futures to complete
    concurrent.futures.wait(metr_list)
    # create an empty list to hold the dataframe
    processed_metric = []
    processed_estimate = []
    # get the elements from the queue
    while not metric_result_queue.empty():
        processed_metric.append(metric_result_queue.get())
    while not estimate_result_queue.empty():
        processed_estimate.append(estimate_result_queue.get())
    # convert the processed_metric list to dataframe
        
    metric_df = pd.concat(processed_metric,ignore_index=True)
    estimate_df = pd.concat(processed_estimate,ignore_index=True)
    return [metric_df,estimate_df]
    




# Getting  Metrics
def get_metrics(params,regions):

    athena_tablename = params['athena_tablename']
    athena_database = params['athena_database']
    athena_bucket=params['athena_bucket']
    athena_bucket_prefix = params['athena_bucket_prefix']
    action_type = params['action']
    provisioned_period = 3600
    consumed_period = 60
    readutilization = params['dynamodb_read_utilization']
    writeutilization = params['dynamodb_write_utilization']
    dynamodb_tablename = params['dynamodb_tablename']
    interval = params['number_of_days_look_back'] 
    now = params['cloudwatch_metric_end_datatime']
    now = datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
    endtime = now 
    accountid = params['accountid']
    starttime=endtime - timedelta(days=interval)
    endtime = endtime.strftime('%Y-%m-%dT%H:%M:%SZ')
    starttime = starttime.strftime('%Y-%m-%dT%H:%M:%SZ')


    if athena_database not in wr.catalog.databases().values:
            wr.catalog.create_database(athena_database)

    def write_to_s3(df, location, mode, table):
        wr.s3.to_parquet(df=df, path=location, database=athena_database, dataset=True, mode=mode, table=table)

    def create_athena_table(tablename):
        wr.catalog.table(database=athena_database, table=tablename)
    results_metrics = []
    results_estimates = []
    for region in regions:
        metrics = list_metrics(dynamodb_tablename,region)
        result = get_table_metrics(metrics, starttime, endtime, consumed_period, provisioned_period, accountid, readutilization, writeutilization,region)
        results_metrics.append(result[0])
        results_estimates.append(result[1])

    results_metrics_df = pd.concat(results_metrics,ignore_index=True)
    results_estimates_df = pd.concat(results_estimates,ignore_index=True)

    if action_type == 'insert':
        print("appending to existing table")
        location_estimate = 's3://{}/{}/{}/estimate/'.format(athena_bucket, athena_bucket_prefix, athena_tablename)
        write_to_s3(results_estimates_df, location_estimate, 'append', athena_tablename+'estimate')
        create_athena_table(athena_tablename+'estimate')

        location_metrics = 's3://{}/{}/{}/metrics/'.format(athena_bucket, athena_bucket_prefix, athena_tablename) 
        write_to_s3(results_metrics_df, location_metrics, 'append', athena_tablename)
        create_athena_table(athena_tablename)
    else:
        print("overwriting to existing table")
        location_estimate = 's3://{}/{}/{}/estimate/'.format(athena_bucket, athena_bucket_prefix, athena_tablename) 
        write_to_s3(results_estimates_df, location_estimate, 'overwrite', athena_tablename+'estimate')
        create_athena_table(athena_tablename+'estimate')

        location_metrics = 's3://{}/{}/{}/metrics/'.format(athena_bucket, athena_bucket_prefix, athena_tablename) 
        write_to_s3(results_metrics_df, location_metrics, 'overwrite', athena_tablename)
        create_athena_table(athena_tablename)

    return 'SUCCEEDED'

