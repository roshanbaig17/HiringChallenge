# HiringChallenge


Install and setup Kafka locally (https://kafka.apache.org/quickstart)

For MacOS, go to the installed Kafka folder and follow these steps

1. Open Terminal and start zookeeper
   - bin/zookeeper-server-start.sh config/zookeeper.properties


2. Open second Terminal and start Kafka server
   - bin/kafka-server-start.sh config/server.properties

3. Open third Terminal and Create topic to consume in python application
   - bin/kafka-topics.sh --create --topic consumerTopic --bootstrap-server localhost:9092


4. Create topic to produce data from python application
   - bin/kafka-topics.sh --create --topic producerTopic --bootstrap-server localhost:9092

To Download the dummy data please use this Link
- http://tx.tamedia.ch.s3.amazonaws.com/challenge/data/stream.jsonl.gz

Now we need to run the code and start consuming data

1. Open terminal in root folder of project and Run this command to create virtual env
   - python3 -m venv venv

2. Run this command to activate virtual env
   - source venv/bin/activate

3. Run this command to install dependencies
   - pip install -r requirements

4. Open project and run main.py

5. Open Terminal where Kafka is installed, run this command to push dummy data so python application can consume it (Change path/to/data with path where data is located)
   - gzcat path/to/data | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic consumerTopic


Report:

Pandas is being used because it provides dataframes which makes it easier to implement grouping, uniqueness and to count the unique ids.

Approach:

Converting received json string from Kafka stream to json object, getting the required 'ts' and 'uid' column data and converting it to DataFrame to generate metrics.
Producing data after every 5 seconds because at that time we have 99.99% of correct data. We read the Kafka stream, convert it to DataFrame and after every 5 seconds produce the results.




