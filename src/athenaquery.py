import boto3
import athena


def create_reserved_cost(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']

    intialqu = """CREATE OR REPLACE VIEW %s_resrvation_recommendation AS 
        SELECT
        "c"."metric_name" "metric_name"
        , "p"."max" "estmax"
        , "p"."min" "estmin"
        , "p"."avg" "estavg"
        , "c"."max" "max"
        , "c"."min" "min"
        , "c"."avg" "avg"
        , "p"."reservedcost" "estreservedcost"
        , "p"."normalcost" "estnormalcost"
        , "c"."reservedcost" "reservedcost"
        , "c"."normalcost" "normalcost"
        , "p"."totalreservedcost" "totalestreservedcost"
        , "p"."totalnormalcost" "totalestnormalcost"
        , "c"."totalreservedcost" "totalreservedcost"
        , "c"."totalnormalcost" "totalnormalcost"
        FROM
        ((
        SELECT
            "max"("provisionedunit") "max"
        , "min"("provisionedunit") "min"
        , "avg"("provisionedunit") "avg"
        , "Provisionedmetric_name"
        , (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 5.9E-5) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 2.99E-4) ELSE 0 END) "reservedcost"
        , (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 1.3E-4) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 6.5E-4) ELSE 0 END) "normalcost",
            (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 5.9E-5) * count(timestamp) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 2.99E-4) * count(timestamp) ELSE 0 END) "totalreservedcost"
        , (CASE WHEN ("Provisionedmetric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("provisionedunit") * 1.3E-4) * count(timestamp) WHEN ("Provisionedmetric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("provisionedunit") * 6.5E-4) * count(timestamp) ELSE 0 END) "totalnormalcost"
        FROM
            (
            SELECT
                "Provisionedmetric_name"
            , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
            , "sum"("provisionedunit") "provisionedunit"
            FROM
                "default"."%s_cost_estimate"
            where ("ondemandcost") > ("provisionedcost")
            GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "Provisionedmetric_name"
        ) 
        GROUP BY "Provisionedmetric_name"
        )  p
        right JOIN (
        SELECT
            "max"("unit") "max"
        , "min"("unit") "min"
        , "avg"("unit") "avg"
        , "metric_name"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 5.9E-5) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 2.99E-4) ELSE 0 END) "reservedcost"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 6.5E-4) ELSE 0 END) "normalcost"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 5.9E-5)  * count(timestamp) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 2.99E-4) * count(timestamp) ELSE 0 END) "totalreservedcost"
        , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("min"("unit") * 1.3E-4) * count(timestamp) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("min"("unit") * 6.5E-4) * count(timestamp) ELSE 0 END) "totalnormalcost"
        FROM
            (
            SELECT
                "metric_name"
            , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
            , "sum"("unit") "Unit"
            FROM
                "default"."%s"
            WHERE ("metric_name" IN ('ProvisionedWriteCapacityUnits', 'ProvisionedReadCapacityUnits'))
            GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "metric_name"
        ) 
        GROUP BY "metric_name"
        )  c ON ("p"."provisionedmetric_name" = "c"."metric_name"))"""
    resrvqu = intialqu % (tablename, tablename, tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': resrvqu
    }

    session = boto3.Session()

    ## Fucntion for obtaining query results and location 
    status = athena.query_results(session, params)
    return status[0]


def create_cost_estimate(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']
    mins = params['dynamodb_minimum_units']

    intialqu = """CREATE OR REPLACE VIEW %s_cost_estimate AS 
                SELECT
                *
                , split_part("name",':',1) as basetable
                , (CASE WHEN ("mode" = 'Ondemand') THEN "ondemandcost" ELSE null END) "ondemandactualcost"
                , (CASE WHEN ("mode" = 'Provisioned') THEN "provisionedcost" ELSE null END) "provisionedactualcost"
                FROM
                (
                SELECT
                    "c"."name"
                , "c"."accountid"
                , "c"."timestamp"
                , "c"."metric_name"
                , "c"."cost" "ondemandcost"
                , "p"."estcost" "provisionedcost"
                , "p"."metric_name" "Provisionedmetric_name"
                , "p"."mode"
                , "c"."unit" "ondemandunit"
                , "p"."estunit" "provisionedunit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "accountid"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN null ELSE null END) "mode"
                    , "unit"
                    , "estunit"
                    
                    /*Dynamo Cost calc */
                    FROM
                        (   
                        SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."accountid"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "accountid"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sestimate"
                    WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                )  p
                LEFT JOIN default.%s ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "accountid"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                ) 
                        )
                    /*Dynamo Cost calc end */
                    WHERE ("metric_name" IN ('ConsumedWriteCapacityUnits'))
                )  c
                LEFT JOIN (
                    SELECT
                        "name"
                    , "accountid"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN 'Ondemand' ELSE 'Provisioned' END) "mode"
                    , "unit"
                    , "estunit"
                    /*Dynamo Cost calc */
                    from (
                    SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."accountid"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "accountid"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sestimate"
                    WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                )  p
                LEFT JOIN "%s" ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "accountid"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedWriteCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                ) 
                    )    
                    /*Dynamo Cost calc end */
                    
                    WHERE ("metric_name" IN ('ProvisionedWriteCapacityUnits'))
                )  p ON (("c"."timestamp" = "p"."timestamp") AND ("c"."name" = "p"."name")))
                    
                UNION    SELECT
                    "c"."name"
                , "c"."accountid"
                , "c"."timestamp"
                , "c"."metric_name"
                , "c"."cost"
                , "p"."estcost"
                , "p"."metric_name" "estmetric_name"
                , "p"."mode"
                , "c"."unit"
                , "p"."estunit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "accountid"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN null ELSE null END) "mode"
                    , "unit"
                    , "estunit"
                    /*Dynamo Cost calc */
                    from (
                    SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."accountid"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "accountid"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sestimate"
                    WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                )  p
                LEFT JOIN "%s" ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "accountid"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                ) 
                    )
                    /*Dynamo Cost calc end */
                    WHERE ("metric_name" IN ('ConsumedReadCapacityUnits'))
                )  c
                LEFT JOIN (
                    SELECT
                        "name"
                    , "accountid"
                    , "timestamp"
                    , "metric_name"
                    , "cost"
                    , "estcost"
                    , (CASE WHEN ("cost" IS NULL) THEN 'Ondemand' ELSE 'Provisioned' END) "mode"
                    , "unit"
                    , "estunit"
                    /*Dynamo Cost calc */
                    from (
                    SELECT
                *
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("estunit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("EstUnit" * 6.5E-4) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ProvisionedReadCapacityUnits') THEN ("Unit" * 1.3E-4) WHEN ("metric_name" = 'ProvisionedWriteCapacityUnits') THEN ("Unit" * 6.5E-4) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "p"."name"
                , "p"."accountid"
                , "p"."timestamp"
                , "p"."metric_name"
                , (CASE WHEN ("%s"."unit" IS NULL) THEN "p"."estunit" ELSE "%s"."unit" END) "EstUnit"
                , "%s"."unit"
                FROM
                    ((
                    SELECT
                        "name"
                    , "accountid"
                    , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                    , (CASE WHEN ("avg"("estUnit") < %s) THEN %s ELSE "avg"("estUnit") END) "EstUnit"
                    , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
                    FROM
                        "%sestimate"
                    WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                    GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name","accountid"
                )  p
                LEFT JOIN "%s" ON ((("p"."timestamp" = "%s"."timestamp") AND ("p"."name" = "%s"."name")) AND ("p"."metric_name" = "%s"."metric_name")))
                ) 
                UNION SELECT
                *
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "estcost"
                , (CASE WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN (("unit" / 1000000) * 1.25) WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN (("unit" / 1000000) * 0.25) ELSE 0 END) "cost"
                FROM
                (
                SELECT
                    "name"
                , "accountid"
                , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
                , "metric_name"
                , "sum"("Unit") "EstUnit"
                , "sum"("unit") "Unit"
                FROM
                    %s
                WHERE ("metric_name" = 'ConsumedReadCapacityUnits')
                GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name" , "accountid"
                ) 
                    )
                    /*Dynamo Cost calc end */
                    WHERE ("metric_name" IN ('ProvisionedReadCapacityUnits'))
                )  p ON (("c"."timestamp" = "p"."timestamp") AND ("c"."name" = "p"."name")))
                )"""
    costqu = intialqu % (
        tablename, tablename, tablename, tablename, mins, mins, tablename, tablename, tablename, tablename,
        tablename, tablename, tablename, tablename, tablename, mins, mins,
        tablename, tablename, tablename, tablename, tablename, tablename, tablename,
        tablename, tablename, mins, mins, tablename, tablename, tablename, tablename, tablename, tablename,
        tablename, tablename, tablename, mins, mins, tablename, tablename, tablename, tablename, tablename, tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costqu
    }

    session = boto3.Session()
    ## Fucntion for obtaining query results and location 
    status = athena.query_results(session, params)
    return status[0]


def create_dynamo_mode_recommendation(params):
    tablename = params['athena_tablename']
    database = params['athena_database']
    bucket = params['athena_bucket']
    path = params['athena_bucket_prefix']
    
    

    intialqu = """CREATE OR REPLACE VIEW %s_recommendation AS 
            SELECT
            "index_name"
            , "base_table_name"
            , "accountid"
            , "provisioned_cost"
            , "Ondemand_cost"
            , "current_mode"
            , "recommended_mode"
            , "diff" "difference_percentage"
            , (CASE WHEN ("recommended_mode" = 'Provisioned') THEN "provisioned_cost" WHEN ("recommended_mode" = 'Ondemand') THEN "Ondemand_cost" ELSE 0 END) "estimated_cost"
            , (CASE WHEN ("current_mode" = 'Provisioned') THEN "provisioned_cost" WHEN ("current_mode" = 'Ondemand') THEN "Ondemand_cost" ELSE 0 END) "current_cost"
            , "number_of_days"
            FROM
            (
            SELECT
                *
            , (CASE WHEN (("recommended_mode" <> "current_mode") AND ("recommended_mode" = 'Provisioned')) THEN ((100 * ("provisioned_cost" - "Ondemand_cost")) / "Ondemand_cost") WHEN (("recommended_mode" <> "current_mode") AND ("recommended_mode" = 'Ondemand')) THEN ((100 * ("Ondemand_cost" - "provisioned_cost")) / "provisioned_cost") ELSE null END) "diff"
            FROM
                (
                SELECT
                    "name" "index_name"
                , "basetable" "base_table_name"
                , "accountid"
                , "sum"("provisionedcost") "provisioned_cost"
                , "sum"("ondemandcost") "Ondemand_cost"
                , "mode" "current_mode"
                , (CASE WHEN ("sum"("ondemandcost") < "sum"("provisionedcost")) THEN 'Ondemand' WHEN ("sum"("ondemandcost") > "sum"("provisionedcost")) THEN 'Provisioned' ELSE null END) "recommended_mode"
                , EXTRACT(DAY FROM (MAX(timestamp) - MIN(timestamp))) "number_of_days"
                FROM
                %s_cost_estimate
            GROUP BY "name", "mode","basetable","accountid"
            ))"""
    costmodequ = intialqu % (tablename, tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': costmodequ
    }

    session = boto3.Session()
    ## Fucntion for obtaining query results and location 
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
   , metric_name
   , accountid
   , (CASE WHEN (min(estUnit) <= 0) THEN 1 ELSE min(estUnit) END) est_min_unit
   , min(min_capacity) current_min_unit
   FROM
     (
      SELECT
        "p"."name"
      , "p"."accountid"
      , "p"."timestamp"
      , "p"."metric_name"
      , "%s_dynamodb_info"."min_capacity"
      , "p"."EstUnit"
      FROM
        ((
         SELECT
           "name"
         , "accountid"
         , "date_trunc"('hour', CAST("timestamp" AS timestamp)) "timestamp"
         , (CASE WHEN ("metric_name" = 'ConsumedReadCapacityUnits') THEN 'ProvisionedReadCapacityUnits' WHEN ("metric_name" = 'ConsumedWriteCapacityUnits') THEN 'ProvisionedWriteCapacityUnits' ELSE "metric_name" END) "metric_name"
         , "avg"("estUnit") "EstUnit"
         FROM
           %sestimate
            WHERE metric_name IN ('ConsumedReadCapacityUnits', 'ConsumedWriteCapacityUnits')
         GROUP BY "date_trunc"('hour', CAST("timestamp" AS timestamp)), "name", "metric_name", "accountid"
      )  p
      LEFT JOIN "%s_dynamodb_info" ON (("p"."name" = "%s_dynamodb_info"."index_name") AND ("p"."metric_name" = "%s_dynamodb_info"."metric_name")))
   ) 
   GROUP BY name, metric_name, accountid
) 
WHERE (current_min_unit > est_min_unit)"""
    as_rec = intialqu % (tablename, tablename,tablename,tablename,tablename,tablename)
    params = {
        'database': database,
        'bucket': bucket,
        'path': path,
        'query': as_rec
    }

    session = boto3.Session()
    ## Fucntion for obtaining query results and location 
    status = athena.query_results(session, params)
    return (status[0])
