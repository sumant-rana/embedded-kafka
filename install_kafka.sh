#!/bin/sh
wget https://dlcdn.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz -O kafka.tgz
mkdir -p kafka
tar xzf kafka.tgz -C kafka --strip-components 1
