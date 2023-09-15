To create and upload docker image:

cd src

export REGION=us-east-2
export STACK_NAME=xapi-quasar-extractor 

docker build  --platform=linux/amd64 -t extractor .
docker tag extractor $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.$REGION.amazonaws.com/$STACK_NAME-repository

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.$REGION.amazonaws.com

docker push $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.$REGION.amazonaws.com/$STACK_NAME-repository\n

Season to taste w/ REGION and STACK_NAME

This is temporary manual method: will be adding dcod builder support soon
