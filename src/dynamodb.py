
import boto3
import pandas as pd
import awswrangler as wr


def get_dynamodb_autoscaling_settings(base_table_name, region, index_name=None):
    # Create a DynamoDB and application-autoscaling client
    app_autoscaling = boto3.client(
        'application-autoscaling', region_name=region)

    resource_id = f"table/{base_table_name}"
    if index_name:
        resource_id = f"{resource_id}/index/{index_name}"
    # Get the current autoscaling settings for the table
    response = app_autoscaling.describe_scalable_targets(
        ResourceIds=[resource_id], ServiceNamespace='dynamodb')
    autoscaling_settings = response['ScalableTargets']
    data = []
    for setting in autoscaling_settings:
        # Get the scaling policy for the setting
        policy_response = app_autoscaling.describe_scaling_policies(
            ServiceNamespace='dynamodb',
            ResourceId=setting['ResourceId'],
            ScalableDimension=setting['ScalableDimension']
        )
        policy = policy_response['ScalingPolicies'][0]["TargetTrackingScalingPolicyConfiguration"]


        data.append({
            'base_table_name': base_table_name,
            'index_name': index_name,
            'metric_name': setting['ScalableDimension'],
            'min_capacity': setting['MinCapacity'],
            'max_capacity': setting['MaxCapacity'],
            'target_utilization': policy['TargetValue'],
            'region': region
        })
    df = pd.DataFrame(data)
    return df


def get_all_dynamodb_autoscaling_settings_with_indexes(region):
    # Create a DynamoDB and application-autoscaling client
    dynamodb = boto3.client('dynamodb', region_name=region)
    app_autoscaling = boto3.client(
        'application-autoscaling', region_name=region)
    # Get a list of all DynamoDB tables
    response = dynamodb.list_tables()
    base_table_names = response['TableNames']

    settings_list = []
    for base_table_name in base_table_names:
        # Get the current provisioned throughput mode for the table
        desc_table = dynamodb.describe_table(TableName=base_table_name)
        if 'ProvisionedThroughput' not in desc_table['Table']:
            continue

        # Get the global secondary indexes (if any)
        global_indexes = desc_table.get('Table', {}).get(
            'GlobalSecondaryIndexes', [])

        # Check if autoscaling is enabled for the table
        try:
            response = app_autoscaling.describe_scalable_targets(
                ResourceIds=[f"table/{base_table_name}"], ServiceNamespace='dynamodb')
            if len(response['ScalableTargets']) == 0:
                continue
            settings_list.append(get_dynamodb_autoscaling_settings(
                base_table_name=base_table_name, region=region))
        except app_autoscaling.exceptions.ValidationException as e:
            if 'does not exist' in e.response['Error']['Message']:
                continue
            else:
                raise e

        for index in global_indexes:
            index_name = index['IndexName']
            # Check if autoscaling is enabled for the index
            try:
                response = app_autoscaling.describe_scalable_targets(
                    ResourceIds=[f"table/{base_table_name}/index/{index_name}"], ServiceNamespace='dynamodb')
                if len(response['ScalableTargets']) == 0:
                    continue
                settings_list.append(get_dynamodb_autoscaling_settings(
                    base_table_name=base_table_name, region=region, index_name=index_name))
            except app_autoscaling.exceptions.ValidationException as e:
                if 'does not exist' in e.response['Error']['Message']:
                    continue
                else:
                    raise e
    return settings_list


def get_dynamodb_scaling_info(params):
    athena_tablename = params['athena_tablename']
    athena_database = params['athena_database']
    athena_bucket = params['athena_bucket']
    athena_bucket_prefix = params['athena_bucket_prefix']
    regions = params['regions']
    all_autoscaling_info = []
    for region in regions:

        all_settings_df = get_all_dynamodb_autoscaling_settings_with_indexes(
            region)
        if len(all_settings_df) > 0:
            all_settings_df = pd.concat(all_settings_df, ignore_index=True)
            all_autoscaling_info.append(all_settings_df)
        else:
            print('No DynamoDB for table in: ' + region)
    if len(all_autoscaling_info) > 0:
        all_autoscaling_info = pd.concat(
            all_autoscaling_info, ignore_index=True)
        all_autoscaling_info['index_name'] = all_autoscaling_info.apply(lambda x: x['base_table_name'] if pd.isnull(
            x['index_name']) else x['base_table_name'] + ':' + x['index_name'], axis=1)
        all_autoscaling_info['metric_name'] = all_autoscaling_info['metric_name'].replace(
            {'dynamodb:table:ReadCapacityUnits': 'ProvisionedReadCapacityUnits', 'dynamodb:index:ReadCapacityUnits': 'ProvisionedReadCapacityUnits'}, regex=True)
        all_autoscaling_info['metric_name'] = all_autoscaling_info['metric_name'].replace(
            {'dynamodb:table:WriteCapacityUnits': 'ProvisionedWriteCapacityUnits', 'dynamodb:index:WriteCapacityUnits': 'ProvisionedWriteCapacityUnits'}, regex=True)
    else:
        all_autoscaling_info = None

    if all_autoscaling_info is not None:
        if athena_database not in wr.catalog.databases().values:
            wr.catalog.create_database(athena_database)

        def write_to_s3(df, location, mode, table):
            wr.s3.to_parquet(df=df, path=location, database=athena_database,
                             dataset=True, mode=mode, table=table)

        def create_athena_table(tablename):
            wr.catalog.table(database=athena_database, table=tablename)

        print("writing dynamodb table information to S3 and Athena")
        location_estimate = 's3://{}/{}/{}/dynamodbtables/'.format(
            athena_bucket, athena_bucket_prefix, athena_tablename)
        write_to_s3(all_autoscaling_info, location_estimate,
                    'overwrite', athena_tablename+'_dynamodb_info')
        create_athena_table(athena_tablename+'_dynamodb_info')
        regions_with_table = all_autoscaling_info['region'].unique()
        return {'status': 'SUCCEEDED', 'regions': regions_with_table}
    else:
        return 'No Table Found'
