# README

This code is a Lambda function that fetches CloudWatch Metrics for DynamoDB tables, estimates units, and generates cost and reservation recommendations.

## Dependencies

This function requires the following libraries to be installed:

- boto3
- os
- pandas
- awswrangler

## Functionality

The main function `lambda_handler` takes in two parameters: `ddbname` and `accountid`.

- `ddbname` is the name of the DynamoDB table for which the metrics are to be fetched.

  - If the `ddbname` parameter is set to 'all', the function will retrieve metrics for all DynamoDB tables in the specified AWS account.
  - If the `ddbname` parameter is set to a specific table name, the function will retrieve metrics only for that table.

- `accountid` is the AWS account ID for which the metrics are to be fetched.

The function first initializes the parameters, including the action, the account ID, and the DynamoDB table name. It then calls the `get_metrics` function, which is responsible for fetching the CloudWatch metrics for the DynamoDB table. The function also has the provision to estimate the units, estimate the cost and create recommendations table in Athena.

If any exception occurs while executing the code, the error message is printed and returned as the response.

## Usage

To use this function, you will need to create a Lambda function in your AWS account, and then upload this code as the function code. You will also need to provide the necessary environment variables and IAM role for the function to access the necessary services.

You can invoke this function using an event source, such as an API Gateway or SNS topic.

You can also test the function by invoking it with the required parameters and testing the output.

# Deployment

You can deploy this function as a zip file by CloudFormation template that creates the necessary resources such as the Lambda function, IAM role, and environment variables.

## Zipping the function folder

To deploy your function using CloudFormation, you will need to package the source code in a zip file.

1. Navigate to the root directory of your function's source code
2. Run the following command to create a zip file of the `src` folder:

  ```sh
  zip -r function.zip src/
  ```

3. Upload the `function.zip` file to an S3 bucket. Make sure to update the CloudFormation template to reference this S3 object.

  ```sh
  aws s3 cp function.zip s3://<your-bucket-name>/function.zip
  ```

4. Deploy the CloudFormation template and pass the bucket name and function s3 key as parameters

  ```sh
  aws cloudformation deploy --template-file deployment.yaml --stack-name <your-stack-name> --parameter-overrides LambdaFunctionS3Bucket=<your-bucket-name>, LambdaFunctionS3Key=function.zip
  ```

## Note

This code is intended to be used as a starting point and may require additional modifications and error handling to fit your specific use case.

It's important to keep in mind that this function is just a sample and you should always test and verify the function before use it in production.
