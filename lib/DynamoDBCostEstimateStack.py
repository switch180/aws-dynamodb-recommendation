import json


from aws_cdk import (Aws,Duration, aws_iam as iam, aws_lambda as _lambda, aws_s3 as s3,Stack)



from constructs import Construct

class AwsDynamodbRecommendationCdkStack(Stack):


    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        athena_prefix = self.node.try_get_context("athena_prefix")
        athena_database = self.node.try_get_context("athena_database")
        athena_table_name = self.node.try_get_context("athena_table_name")

        # S3 Bucket
        s3_bucket = s3.Bucket(self, "S3Bucket")
        
        # DynamoDB Cost Estimate Role
        dynamodb_cost_estimate_role = iam.Role(
            self, "DynamoDBCostEstimateRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
        )
        
        # Athena CloudWatch Role
        athena_cloudwatch_role = iam.Policy(
            self, "AthenaCloudWatchRole",
            policy_name="AthenaCloudWatchRole",
            roles=[dynamodb_cost_estimate_role],
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "cloudwatch:Describe*",
                        "cloudwatch:Get*",
                        "cloudwatch:List*",
                        "athena:GetQueryExecution"
                    ],
                    resources=["*"],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    actions=[
                        "glue:*",
                        "athena:*"
                    ],
                    resources=["*"],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    actions=["s3:*"],
                    resources=[
                        s3_bucket.bucket_arn,
                        s3_bucket.arn_for_objects("*"),
                    ],
                    effect=iam.Effect.ALLOW
                ),
            ],
        )
        
        # DynamoDBAccess
        dynamodb_access = iam.Policy(
            self, "DynamoDBAccess",
            policy_name="DynamoDBAccess",
            roles=[dynamodb_cost_estimate_role],
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "dynamodb:ListTables",
                        "dynamodb:DescribeTable",
                        "application-autoscaling:DescribeScalableTargets",
                        "application-autoscaling:DescribeScalingActivities",
                        "application-autoscaling:DescribeScalingPolicies",
                        "application-autoscaling:DescribeScheduledActions"
                    ],
                    resources=["*"],
                    effect=iam.Effect.ALLOW
                )
            ]
        )
        
        # DynamoDB Cost Estimate Function
        dynamodb_cost_estimate_function = _lambda.Function(
            self, "DynamoDBCostEstimateFunction",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("src"),
            environment={
                "ATHENA_BUCKET": s3_bucket.bucket_name,
                "ATHENA_PREFIX": athena_prefix,
                "ATHENA_DATABASE": athena_database,
                "ATHENA_TABLENAME": athena_table_name
            },
            role=dynamodb_cost_estimate_role,
            timeout=Duration.seconds(900),
            memory_size=4096
            
            
        )
        AWSSDKPandasLayer = _lambda.LayerVersion.from_layer_version_arn(self, "AWSSDKPandas", layer_version_arn=f'arn:aws:lambda:{Aws.REGION}:336392948345:layer:AWSSDKPandas-Python38:3')
        dynamodb_cost_estimate_function.add_layers(AWSSDKPandasLayer)
      
