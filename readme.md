# README

This is a AWS Lambda function that uses CloudWatch metrics to create cost estimates, DynamoDB throughput mode recommendations, and reserved cost recommendations for DynamoDB table(s).

## Parameters

The function takes the following parameters in the event:

- `action`: The action to perform. Accepted values are create or insert. create will recreate everything and insert will append the new metrics to existing metrics.
- `accountid`: The AWS account ID of the DynamoDB table.
- `dynamodb_tablename`: The name of the DynamoDB table to create cost estimates and recommendations for.
- `dynamodb_read_utilization`: The read utilization percentage threshold to use when creating recommendations.
- `dynamodb_write_utilization`: The write utilization percentage threshold to use when creating recommendations.
- `dynamodb_minimum_units`: The minimum number of read and write capacity units to use when creating recommendations.
- `number_of_days_look_back`: The number of days to look back in CloudWatch metrics when creating cost estimates and recommendations. Maximum number of days is 14 due to CloudWatch metrics limitations
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
3. Recommends the best provisioned mode for the DynamoDB table based on the cost estimate
4. Recommends the best reserved capacity settings for the DynamoDB table based on the cost estimate

**Please note that the simulation of DynamoDB usage is based on the provided metrics and other assumptions, and it might be different from the actual usage.**
# Deployment

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

## Usage

To invoke this Lambda function, you can use the AWS Lambda service in the AWS Management Console, the AWS Command Line Interface (CLI), or one of the AWS SDKs. Here are examples of how you can invoke the function using the AWS CLI :

Using the AWS CLI:

  ```sh
  aws lambda invoke --function-name my_lambda_function --payload '{"action":"create","accountid":"123456789","dynamodb_tablename":"my_table","dynamodb_read_utilization":"70","dynamodb_write_utilization":"70","dynamodb_minimum_units":"5","number_of_days_look_back":"30","cloudwatch_metric_end_datatime":"2022-01-01"}' response.json
  ```

## Note

This code is intended to be used as a starting point and may require additional modifications and error handling to fit your specific use case.

It's important to keep in mind that this function is just a sample and you should always test and verify the function before use it in production.
