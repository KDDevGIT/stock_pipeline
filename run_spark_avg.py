#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_processor.py
import subprocess

subprocess.run(
        [
            "spark-submit",  # Use full path if needed, e.g., r"C:\path\to\spark-submit.cmd"
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "spark_processor.py"  # Ensure this file is in the current working directory
        ],
        shell=True,  # Needed on Windows to correctly interpret the command
    )