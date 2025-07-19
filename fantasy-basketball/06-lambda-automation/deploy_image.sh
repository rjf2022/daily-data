
# define variables 
ACCOUNT_ID="YOUR_ACCOUNT_ID"             # replace with your AWS account ID 
REGION="us-east-2"                       # replace with your desired AWS region 
REPO_NAMESPACE="demos"                   # replace with your ECR repository namespace
FUNCTION_NAME="lambda-function-code"     # replace with your Lambda function name

# login to ecr
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# build the docker image 
docker build --platform linux/arm64 --provenance=false -t $FUNCTION_NAME . 

# tag the docker image
docker tag $FUNCTION_NAME:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAMESPACE/$FUNCTION_NAME:latest

# push the docker image to ecr
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAMESPACE/$FUNCTION_NAME:latest
