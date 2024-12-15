# stock_pipeline

<u>Dependencies</u>
pip install kafka-python requests
docker exec -it kafka bash
<u>List Topics</u>
kafka-topics --bootstrap-server localhost:9092 --list
<u>Create Topic</u>
kafka-topics --create --topic stock-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
<u>Build Docker Image</u>
docker build -t stock-producer .
<u>Run Container Image</u>
docker run --network kafka-network stock-producer
<u>Access Container Bash</u>
docker run -it --rm --network kafka-network stock-producer bash
<u>Test Connection</u>
apt-get update && apt-get install -y telnet
telnet kafka 9092
