#rebuild/restart container (per .yaml config)
docker-compose up -d --build
[+] Running 2/2
 ✔ Container kafka      Started                                                                                                 1.4s
 ✔ Container zookeeper  Started  

#verify services are running
docker-compose ps
NAME        IMAGE                      COMMAND                  SERVICE     CREATED          STATUS          PORTS
kafka       confluentinc/cp-kafka      "/etc/confluent/dock…"   kafka       22 seconds ago   Up 20 seconds   0.0.0.0:9092->9092/tcp
zookeeper   bitnami/zookeeper:latest   "/opt/bitnami/script…"   zookeeper   57 seconds ago   Up 21 seconds   2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp, 8080/tcp

#Enter container bash
docker exec -it kafka bash

#List Kafka container topics (in bash)
kafka-topics --bootstrap-server localhost:9092 --list

#List Kafka container topic contents (in bash)
kafka-console-consumer --bootstrap-server localhost:9092 --topic stock_prices --from-beginning