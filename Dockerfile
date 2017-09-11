# FROM ubuntu:latest
FROM alpine:latest
WORKDIR /root
RUN apk update && \
    apk upgrade && \
    apk add unzip
ADD target/x86_64-unknown-linux-musl/release/travels-task .
# RUN apt-get -y update && apt-get -y install unzip
# ADD target/release/travels-task .
EXPOSE 80
CMD cp /tmp/data/data.zip /root && cd /root && unzip -q data.zip && rm data.zip && mkdir c && ./travels-task
