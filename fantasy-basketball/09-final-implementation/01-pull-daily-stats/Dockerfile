# Use the official AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12

# Install dependencies 
COPY requirements.txt . 
RUN pip install -r requirements.txt 

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT} 

# Copy the modules folder 
COPY modules ${LAMBDA_TASK_ROOT}/modules 

# Set the CMD to your handler (app.lambda_handler)
CMD ["app.lambda_handler"] 