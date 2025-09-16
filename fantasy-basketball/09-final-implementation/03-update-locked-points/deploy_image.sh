
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 474668423427.dkr.ecr.us-east-2.amazonaws.com 

# docker build --no-cache --platform linux/arm64 --provenance=false -t update-locked-points . 
docker build --platform linux/arm64 --provenance=false -t update-locked-points . 

docker tag update-locked-points:latest 474668423427.dkr.ecr.us-east-2.amazonaws.com/fantasy-basketball/update-locked-points:latest 

docker push 474668423427.dkr.ecr.us-east-2.amazonaws.com/fantasy-basketball/update-locked-points:latest 