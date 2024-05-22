# DataPipelineApp

This is a sample serverless DataPipeline application  which process a json file containing weather events uploaded to s3 bucket.


## Deploy the sample application

The Serverless Application Model Command Line Interface (SAM CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications.

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* AWS CLI -[Install and configure AWS CLI]https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
* Java11 - [Install the Java 11](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/downloads-list.html)
* Maven - [Install Maven](https://maven.apache.org/install.html)

To build and deploy your application for the first time, run the following in your shell:
1) Build  and package the  application using **mvn clean package**
2) validate the sam template using  **sam validate -t template.yaml**
3) create a s3 bucket for SAM to manage lambda artifacts by running **aws s3api create-bucket --bucket <BUCKET_NAME> --region us-east-1**
4) Deploy the application using **sam deploy --s3-bucket <BUCKET_NAME>  --stack-name data-pipeline-app  --capabilities CAPABILITY_IAM**