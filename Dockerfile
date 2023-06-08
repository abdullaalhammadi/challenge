# Use the official Python base image
FROM python:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script and the CSV file to the working directory
COPY requirements.txt .

# Install any necessary dependencies
# If your script requires additional packages, list them in a requirements.txt file and uncomment the following lines:
# COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENV API_KEY = 604b89424153b914385fff4e3919f459
EXPOSE 8501
# Run the Python script
CMD ["streamlit", "run", "pipeline.py"]
