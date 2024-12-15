# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the producer script into the container
COPY producer.py .

# Install required dependencies
RUN pip install kafka-python requests

# Command to run the script
CMD ["python", "producer.py"]

