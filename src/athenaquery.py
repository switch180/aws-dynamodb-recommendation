import boto3
import athena


def create_cost_estimate(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']
    write_min = params['dynamodb_minimum_write_unit']
    read_min = params['dynamodb_minimum_read_unit']

    intialqu = """CREATE OR REPLACE VIEW "%s_cost_estimate" AS 
        SELECT
        q1.name
        , q1.accountid
        , q1.region
        , q1.timestamp
        , q1.metric_name
        , q1.Provisioned_Estunit est_provisioned_unit
        , q2.unit provisioned_unit
        , q1.Consumed_unit "Consumed_unit"
        , COALESCE(q2.provisioned_cost, q1.est_provisoned_cost) "provisioned_cost"
        , q1.ondemand_cost "ondemand_cost"
        FROM
        ((
        SELECT
            name
        , accountid
        ,region
        , date_trunc('hour', CAST(timestamp AS timestamp)) "timestamp"
        , sum(unit) "Consumed_unit"
        , avg((CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN (CASE WHEN (estUnit < %s) THEN %s ELSE estUnit END) WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN (CASE WHEN (estUnit < %s) THEN %s ELSE estUnit END) END)) Provisioned_Estunit
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' ELSE null END) metric_name
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN ((sum(unit) / 1000000) * 1.25E0) WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN ((sum(unit) / 1000000) * 2.5E-1) ELSE null END) ondemand_cost
        , (CASE WHEN (metric_name = 'ConsumedWriteCapacityUnits') THEN (avg((CASE WHEN (estUnit < %s) THEN %s ELSE estUnit END)) * 1.3E-4) WHEN (metric_name = 'ConsumedReadCapacityUnits') THEN (avg((CASE WHEN (estUnit < %s) THEN %s ELSE estUnit END)) * 6.5E-4) ELSE null END) est_provisoned_cost
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
        tablename, write_min, write_min,read_min,read_min,write_min, write_min,read_min,read_min,tablename,tablename)
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

    intialqu = """CREATE OR REPLACE VIEW %s_recommendation AS 
        SELECT
            q1.index_name,
            q1.base_table_name,
            q1.accountid,
            q1.provisioned_cost,
            q1.Ondemand_cost,
            q1.recommended_mode,
            q1.number_of_days,
            case when %s_dynamodb_info.base_table_name is not NULL then 'Provisioned' else 'Ondemand' end current_mode
            FROM
            (SELECT
                "name" "index_name",
                split_part("name", ':', 1) "base_table_name",
                "accountid",
                "region",
                "sum"("provisioned_cost") "provisioned_cost",
                "sum"("ondemand_cost") "Ondemand_cost",
                (CASE WHEN ("sum"("ondemand_cost") < "sum"("provisioned_cost")) THEN 'Ondemand'
                    WHEN ("sum"("ondemand_cost") > "sum"("provisioned_cost")) THEN 'Provisioned'
                    ELSE null
                END) "recommended_mode",
                EXTRACT(DAY FROM (MAX(timestamp) - MIN(timestamp))) "number_of_days"
            FROM
                %s_cost_estimate
            GROUP BY "name", split_part("name", ':', 1), "accountid","region"
            ) q1
            LEFT JOIN %s_dynamodb_info
            ON %s_dynamodb_info.base_table_name = q1.base_table_name
            AND %s_dynamodb_info.index_name = q1.index_name
            AND %s_dynamodb_info.region = q1.region"""
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
   , (CASE WHEN (min(estUnit) <= 0) THEN 1 ELSE min(estUnit) END) est_min_unit
   , min(min_capacity) current_min_unit
   FROM
     (
      SELECT
        "p"."name"
        , "p"."region"
      , "p"."accountid"
      , "p"."timestamp"
      , "p"."metric_name"
      , "%s_dynamodb_info"."min_capacity"
      , "p"."EstUnit"
      FROM
        ((
         SELECT
           "name"
           , "region"
         , "accountid"
         , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
         , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
         , "avg"("estUnit") "EstUnit"
         FROM
           %sestimate
            WHERE metric_name IN ('ConsumedReadCapacityUnits', 'ConsumedWriteCapacityUnits')
         GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name", "accountid","region"
      )  p
      LEFT JOIN "%s_dynamodb_info" ON ((((("p"."name" = "%s_dynamodb_info"."index_name") AND ("p"."metric_name" = "%s_dynamodb_info"."metric_name"))) AND ("p"."region" = "%s_dynamodb_info"."region"))))
   ) 
   GROUP BY name, metric_name, accountid,region
) 
WHERE (current_min_unit > est_min_unit)"""
    as_rec = intialqu % (tablename, tablename, tablename,
                         tablename, tablename, tablename, tablename)
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
