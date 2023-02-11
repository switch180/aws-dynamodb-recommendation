# README

This function helps to analyze the usage of DynamoDB tables and provides recommendations for the throughput mode that can be used to optimize cost. It uses CloudWatch metrics to simulate the usage of DynamoDB tables and provides cost estimates for different throughput modes. This function can be a valuable tool for managing DynamoDB tables and ensuring that they are running in an optimal configuration.

By using Athena and QuickSight, you can further analyze the data and gain insights into the usage patterns of your DynamoDB tables. This can help you to identify areas where you can optimize performance, reduce costs, and make better decisions about how to configure your DynamoDB tables.

## Parameters

The function takes the following parameters in the event:

- `action`: The action to perform on the DynamoDB table. Accepted values are `create` or `append`. `create` will recreate the metrics and recommendations from scratch, while `append` will append new metrics to existing metrics.

    *Note:* If using '`append`' action, make sure to change the `cloudwatch_metric_end_datetime` to avoid duplicates.
- `accountid`: The AWS account ID of the DynamoDB table(s) to analyze.
- `regions` : List of AWS Regions to analyze DynamoDB tables, example: `['us-east-1', 'us-east-2']`
- `dynamodb_tablename`: The name of the DynamoDB table to create cost estimates and recommendations for. If set to `all`, the function will analyze all tables in the specified regions.
- `dynamodb_read_utilization`: The read utilization percentage threshold to use when creating recommendations.
- `dynamodb_write_utilization`: The write utilization percentage threshold to use when creating recommendations.
- `dynamodb_minimum_units`: The minimum number of read and write capacity units to use when creating recommendations.
- `number_of_days_look_back`: The number of days to look back in CloudWatch metrics when creating cost estimates and recommendations. Maximum number of days is 14 due to CloudWatch metrics limitations.
- `cloudwatch_metric_end_datatime`: The end time of the CloudWatch metrics to use when creating cost estimates and recommendations.

It also takes the following environment variables:

- `ATHENA_TABLENAME`: The name of the Athena table to create cost estimates and recommendations in.
- `ATHENA_DATABASE`: The name of the Athena database to create cost estimates and recommendations in.
- `ATHENA_BUCKET`: The name of the S3 bucket to use for Athena results.
- `ATHENA_PREFIX`: The prefix of the S3 bucket to use for Athena results.

## Dependencies

This function requires the following libraries to be installed:

- boto3
- awswrangler

## Functionality

The Lambda function performs the following steps:

1. Fetches CloudWatch metrics data for the DynamoDB table
2. Estimates the cost of the DynamoDB table based on the target read and write utilization and minimum capacity units
3. Recommends the best throughput mode for the DynamoDB table based on the usage

**Please note that the simulation of DynamoDB usage is based on the provided metrics and other assumptions, and it might be different from the actual usage.**
# Deployment

This deploys a stack that includes the following resources:

- S3 Bucket
- IAM Role
- Lambda Function

## CloudFormation
You can deploy this function as a zip file by CloudFormation template that creates the necessary resources such as the Lambda function, IAM role, and environment variables.

To deploy your function using CloudFormation, you will need to package the source code in a zip file.

1. Navigate to the root directory of your function's source code
2. Run the following command to create a zip file of the `src` folder:

  ```sh
  cd src
  zip -r function.zip .
  ```

3. Upload the `function.zip` file to an S3 bucket. Make sure to update the CloudFormation template to reference this S3 object.

  ```sh
  aws s3 cp function.zip s3://<your-bucket-name>/function.zip
  ```

4. Deploy the CloudFormation template and pass the bucket name and function s3 key as parameters

  ```sh
  aws cloudformation deploy --template-file deployment.yaml --stack-name dynamodb-estimation --parameter-overrides LambdaFunctionS3Bucket=<your-bucket-name> LambdaFunctionS3Key=<function.zip> --capabilities CAPABILITY_IAM
  ```

## CDK

**Prerequisites**

- [AWS CDK](https://aws.amazon.com/cdk/) installed and configured on your local machine
- [Python](https://www.python.org/downloads/) installed on your local machine
- [AWS CLI](https://aws.amazon.com/cli/) installed and configured on your local machine
- Familiarity with [AWS CDK](https://aws.amazon.com/cdk/) and [AWS Lambda](https://aws.amazon.com/lambda/)

**Deployment**

- Clone the repository
- In the command line, navigate to cloned directory
- Run `cdk synth` to create the CloudFormation template
- Run `cdk deploy` to deploy the stack
- Provide the required context values (`athena_prefix, athena_database, athena_table_name`)

**Cleanup**
To delete the stack and all its associated resources, run `cdk destroy`.
## Usage

To invoke this Lambda function, you can use the AWS Lambda service in the AWS Management Console, the AWS Command Line Interface (CLI), or one of the AWS SDKs. Here are examples of how you can invoke the function using the AWS CLI :

Using the AWS CLI:

  ```sh
  aws lambda invoke --function-name my_lambda_function --payload '{"action":"create","regions": ["us-east-1"], "accountid":"123456789","dynamodb_tablename":"all","dynamodb_read_utilization":70,"dynamodb_write_utilization":70,"dynamodb_minimum_units":5,"number_of_days_look_back":12,"cloudwatch_metric_end_datatime":"2023-01-26 00:00:00"}' response.json
  ```

Use Athena to query the <ATHENA_TABLENAME>_recommendation view and see the recommended throughput mode:

1.  Open the Amazon Athena console.
2.  In the navigation pane, choose "Query Editor".
3.  In the query editor, enter a query to select the recommended throughput mode from the ATHENA_TABLENAME, for example:

  ```sh
  SELECT * FROM <ATHENA_TABLENAME>_recommendation;
  ```

4. Choose "Run Query"
5. The query results will show the recommended throughput mode for the DynamoDB table.

You can also connect the Athena Table to QuickSight and do further analysis.
## Note

This code is intended to be used as a starting point and may require additional modifications and error handling to fit your specific use case.

It's important to keep in mind that this function is just a sample and you should always test and verify the function before use it in production.

# License
This software is provided "as is" and use it at your own risk.
This code is licensed under the MIT License.
