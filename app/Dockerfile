FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3 python3-pip openjdk-11-jdk curl && \
    pip3 install --upgrade pip

COPY . /app
WORKDIR /app

RUN pip3 install -r requirements.txt

EXPOSE 5003

CMD ["python3", "app.py"]
