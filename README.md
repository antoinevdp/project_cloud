# Project Cloud

This project contains a serverless application that exposes an API to get parking information.

## Deployment

To deploy this application, you need to have the AWS SAM CLI installed.

1. **Package the application:**
```bash
sam package --template-file template.yaml --s3-bucket <your-s3-bucket-for-deployment> --output-template-file packaged.yaml
```
Replace `<your-s3-bucket-for-deployment>` with the name of an S3 bucket in your account to store the packaged code.

2. **Deploy the application:**
```bash
sam deploy --template-file packaged.yaml --stack-name <your-stack-name> --capabilities CAPABILITY_IAM
```
Replace `<your-stack-name>` with the name you want to give to your CloudFormation stack.

After deployment, the API Gateway endpoint URL will be displayed in the outputs.


### Improvements

redis to improve performance
choose specific date for the api, not only the last processed data
EC2 always on, find a way to schedule it
