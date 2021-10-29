FROM public.ecr.aws/lambda/python:3.8

# Set the working directory in the container
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy function code
COPY src/ .

# Avoid cache purge by adding requirements first
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

ENV AWS_ACCESS_KEY_ID $AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY $AWS_SECRET_ACCESS_KEY
ENV AWS_DEFAULT_REGION $AWS_DEFAULT_REGION

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]