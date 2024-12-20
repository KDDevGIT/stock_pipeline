import subprocess

# Define the command as a list
import subprocess
#starts Kafka API pull for SINGLE Stock
subprocess.run(["docker", "exec", "-it", "kafka", "kafka-console-consumer", "--bootstrap-server", "localhost:9092", "--topic", "stock_prices", "--from-beginning"])