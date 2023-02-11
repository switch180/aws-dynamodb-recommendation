import boto3
import athena


def create_cost_estimate(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']
    write_min = params['dynamodb_minimum_write_unit']
    read_min = params['dynamodb_minimum_read_unit']
    dynamodb_write_utilization = params['dynamodb_write_utilization']
    dynamodb_read_utilization = params['dynamodb_read_utilization']

    intialqu = """CREATE OR REPLACE VIEW "%s_cost_estimate" AS 
        SELECT
        q1.name
        , q1.accountid
        , q1.region
        , q1.timestamp
        , q1.metric_name
        , q1.Provisioned_Estunit est_provisioned_unit
        , q2.unit provisioned_unit
        , q1.Consumed_unit "ondemand_unit"
        ,q2.provisioned_cost as current_provisioned_cost
        ,q1.est_provisoned_cost "est_provisioned_cost"
        , q1.ondemand_cost "ondemand_cost"
        ,COALESCE(q2.provisioned_cost,q1.ondemand_cost) as current_cost
        , q1.min_capacity
        , q1.target_utilization
        FROM
        ((
        SELECT
            name
        , accountid
        ,region
        , date_trunc('hour', CAST(timestamp AS timestamp)) "timestamp"
        , sum(unit) "Consumed_unit"
        , avg(estUnit) "Provisioned_Estunit"
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN '%s' WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN '%s' ELSE null END) min_capacity
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN '%s' WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN '%s' ELSE null END) target_utilization
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' ELSE null END) metric_name
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN ((sum(unit) / 1000000) * 1.25E0) WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN ((sum(unit) / 1000000) * 2.5E-1) ELSE null END) ondemand_cost
        , (CASE WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN (avg(estUnit) * 1.3E-4) WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN (avg(estUnit) * 6.5E-4) ELSE null END) est_provisoned_cost
        FROM
            %sestimate
        GROUP BY date_trunc('hour', CAST(timestamp AS timestamp)), name, metric_name, accountid,region
        )  q1
        LEFT JOIN (
        SELECT
            name
        , accountid
        ,region
        , date_trunc('hour', CAST(timestamp AS timestamp)) "timestamp"
        , metric_name
        , sum(unit) "unit"
        , (CASE WHEN (metric_name = 'ProvisionedReadCapacityUnits') THEN (avg(unit) * 1.3E-4) WHEN (metric_name = 'ProvisionedWriteCapacityUnits') THEN (avg(unit) * 6.5E-4) ELSE null END) provisioned_cost
        FROM
            %s
        GROUP BY date_trunc('hour', CAST(timestamp AS timestamp)), name, metric_name, accountid,region
        )  q2 ON ((q1.name = q2.name) AND (q1.accountid = q2.accountid) AND (q1.timestamp = q2.timestamp) AND (q1.metric_name = q2.metric_name)))"""
    costqu = intialqu % (
        tablename, write_min, read_min, dynamodb_write_utilization, dynamodb_read_utilization , tablename,tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costqu
    }

    session = boto3.Session()
    # Fucntion for obtaining query results and location
    status = athena.query_results(session, params)
    return status[0]


def create_dynamo_mode_recommendation(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']
  

    intialqu = """CREATE OR REPLACE VIEW "%s_recommendation" AS 
        SELECT
        q1.index_name
        , q1.base_table_name
        , q1.accountid
        , q1.est_provisioned_cost
        , q1.current_provisioned_cost
        , q1.Ondemand_cost
        , q1.recommended_mode
        , (CASE WHEN (%s_dynamodb_info.base_table_name IS NOT NULL) THEN 'Provisioned' ELSE 'Ondemand' END) current_mode
        , q1.number_of_days
        FROM
        ((
        SELECT
            "name" "index_name"
        , split_part("name", ':', 1) "base_table_name"
        , "accountid"
        , "region"
        , "sum"("est_provisioned_cost") "est_provisioned_cost"
        , "sum"("current_provisioned_cost") current_provisioned_cost
        , "sum"("ondemand_cost") "Ondemand_cost"
        , (CASE WHEN (("sum"("est_provisioned_cost") < "sum"("current_provisioned_cost")) AND ((("sum"("current_provisioned_cost") - "sum"("est_provisioned_cost")) / "sum"("current_provisioned_cost")) > 1.5E-1) AND ("sum"("est_provisioned_cost") < "sum"("ondemand_cost"))) THEN 'Provisioned_Modify' WHEN (("sum"("current_provisioned_cost") > "sum"("ondemand_cost")) OR ("sum"("est_provisioned_cost") > "sum"("ondemand_cost"))) THEN 'Ondemand' ELSE 'Provisioned' END) "recommended_mode"
        , EXTRACT(DAY FROM (MAX(timestamp) - MIN(timestamp))) "number_of_days"
        FROM
            %s_cost_estimate
        GROUP BY "name", split_part("name", ':', 1), "accountid", "region"
        )  q1
        LEFT JOIN %s_dynamodb_info ON ((%s_dynamodb_info.base_table_name = q1.base_table_name) AND (%s_dynamodb_info.index_name = q1.index_name) AND (%s_dynamodb_info.region = q1.region)))"""
    costmodequ = intialqu % (tablename, tablename,tablename,tablename,tablename,tablename,tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costmodequ
    }

    session = boto3.Session()
    # Fucntion for obtaining query results and location
    status = athena.query_results(session, params)
    return (status[0])


def create_dynamo_autoscaling_recommendation(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']

    intialqu = """CREATE OR REPLACE VIEW "%s_autoscaling_sizing" AS 
        SELECT *
        FROM
        (
        SELECT
            name
        , "region"
        , metric_name
        , accountid
        , (CASE WHEN (min(estUnit) <= 0) THEN 1 ELSE min(estUnit) END) est_min_capacity
        , min(min_capacity) current_min_capacity
        , min(target_utilization) current_min_target_utilization
        , supplied_min_capacity
        , supplied_target_utilization
        FROM
            (
            SELECT
                "p"."name"
            , "p"."region"
            , "p"."accountid"
            , "p"."timestamp"
            , "p"."metric_name"
            , "%s_dynamodb_info"."min_capacity"
            , "%s_dynamodb_info"."target_utilization"
            , "p"."EstUnit"
            , "p"."supplied_min_capacity"
            , "p"."supplied_target_utilization"
            FROM
                ((
                SELECT
                "name"
                , "region"
                , "accountid"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "avg"("est_provisioned_unit") "estUnit"
                , "min_capacity" "supplied_min_capacity"
                , "target_utilization" "supplied_target_utilization"
                FROM
                %s_cost_estimate
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name", "accountid", "region", min_capacity, target_utilization
            )  p
            LEFT JOIN "%s_dynamodb_info" ON ((("p"."name" = "%s_dynamodb_info"."index_name") AND ("p"."metric_name" = "%s_dynamodb_info"."metric_name")) AND ("p"."region" = "%s_dynamodb_info"."region")))
        ) 
        GROUP BY name, metric_name, accountid, region, supplied_min_capacity, supplied_target_utilization
        ) 
        WHERE (current_min_capacity > est_min_capacity)
        """
    as_rec = intialqu % (tablename, tablename, tablename,
                         tablename, tablename, tablename, tablename,tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': as_rec
    }

    session = boto3.Session()
    # Fucntion for obtaining query results and location
    status = athena.query_results(session, params)
    return (status[0])
