# Kafka-Practice
Producing and Consuming Messages in Apache Kafka
Problem statement: write a Kafka producer to publish log messages into a stream, write a Kafka consumer that subscribes for those log messages, and persist the messages to a file if a absolute value of a threshold is exceeded.  The motivation for such a data flow is to build an alerting system for a set of IoT-managed devices.

Data: generate data from your Kafka producer code. Each log message has the following format:
pump_id,time_stamp,vibration_delta

•	pump_id is an integer provided as a command-line argument
•	time_stamp is provided by java.util.Data.getTime()
•	vibration_delta is sampled from org.apache.commons.math3.distribution.NormalDistribution with mean=0 and standard deviation=0.2
