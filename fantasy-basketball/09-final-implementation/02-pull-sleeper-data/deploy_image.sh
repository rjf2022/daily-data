
# define which lambda function to deploy 
FUNCTION_NAME="pull-sleeper-data" 

# login to ecr 
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 474668423427.dkr.ecr.us-east-2.amazonaws.com 

# build the docker image 
docker build --platform linux/arm64 --provenance=false -t $FUNCTION_NAME . 
# docker build --no-cache --platform linux/arm64 --provenance=false -t $FUNCTION_NAME . 

# tag the docker image 
docker tag $FUNCTION_NAME:latest 474668423427.dkr.ecr.us-east-2.amazonaws.com/fantasy-basketball/$FUNCTION_NAME:latest 

# push the docker image to ecr 
docker push 474668423427.dkr.ecr.us-east-2.amazonaws.com/fantasy-basketball/$FUNCTION_NAME:latest 
