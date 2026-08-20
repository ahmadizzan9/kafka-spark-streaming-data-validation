booting docker
```
docker compose up -d
```

Create a topic 
```
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-topics.sh --create --topic transactions_valid --bootstrap-server localhost:9092
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-topics.sh --create --topic transactions_dlq --bootstrap-server localhost:9092 
```

Verification (TOPIC)
```
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-topics.sh --describe --topic (TOPIC) --bootstrap-server localhost:9092
```

check list (TOPIC)
```
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

delete topic
```
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-topics.sh --delete --topic transactions --bootstrap-server localhost:9092
```

execute producer
```
python producer/producer.py
```
execute streaming pipeline
```
python streaming/spark_streaming_job.py
```

read a messege from topic
```
==== dengan deklarasi group ====
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-console-consumer.sh --topic transactions --from-beginning --group transactions_group --bootstrap-server localhost:9092
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-console-consumer.sh --topic transactions_valid --from-beginning --group transactions_valid_group --bootstrap-server localhost:9092
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-console-consumer.sh --topic transactions_dlq --from-beginning --group transactions_invalid_group --bootstrap-server localhost:9092

==== cek group ====
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092
```
check the topic lag
```
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-consumer-groups.sh --describe --group transactions_group --bootstrap-server localhost:9092
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-consumer-groups.sh --describe --group transactions_valid_group --bootstrap-server localhost:9092
docker exec -it kafka-stream_pipeline /opt/kafka/bin/kafka-consumer-groups.sh --describe --group transactions_invalid_group --bootstrap-server localhost:9092
```

running spark using spark bash
```
docker exec -it spark-job bash

export PATH=$PATH:/opt/spark/bin

spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 /opt/spark-apps/spark_streaming_job.py
```

