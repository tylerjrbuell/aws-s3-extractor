FROM python:3.8-slim-buster
WORKDIR /aws-s3

COPY requirements.txt .
#Install Amazon sdk
RUN pip install -r requirements.txt
#Install os packages
RUN apt-get update && apt-get install -yq curl unzip less
#Install aws cli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip 
RUN ./aws/install

ENTRYPOINT python main.py