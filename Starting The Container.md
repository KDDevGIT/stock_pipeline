#rebuild/restart container (per per docker-compose.yaml config. edit the config to make changes on docker build)
C:\Users\kdabc\stock_pipeline>docker-compose up -d
[+] Running 3/0
 ✔ Container zookeeper  Running                                                                                                 0.0s
 ✔ Container spark      Running                                                                                                 0.0s
 ✔ Container kafka      Running   

#verify services are running
C:\Users\kdabc\stock_pipeline>docker-compose ps
NAME        IMAGE                              COMMAND                  SERVICE     CREATED       STATUS       PORTS
kafka       confluentinc/cp-kafka:latest       "/etc/confluent/dock…"   kafka       2 hours ago   Up 2 hours   0.0.0.0:9092->9092/tcp
spark       bitnami/spark:latest               "/opt/bitnami/script…"   spark       2 hours ago   Up 2 hours   0.0.0.0:8080->8080/tcp
zookeeper   confluentinc/cp-zookeeper:latest   "/etc/confluent/dock…"   zookeeper   2 hours ago   Up 2 hours   2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp

#lauch producer
python kafka_producer.py (or whatever producer version)

#Enter container bash
docker exec -it kafka bash

#List Kafka created topics (in bash)
kafka-topics --bootstrap-server localhost:9092 --list

#can do all as one command 
docker exec -it kafka bash kafka-topics --bootstrap-server localhost:9092 --list

#List Kafka container topic contents being pulled from producer (in bash)
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock_prices --from-beginning

#Spark Job - Moving Average over batches from Kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 C:\Users\kdabc\stock_pipeline\spark_processor.py #this will also work
