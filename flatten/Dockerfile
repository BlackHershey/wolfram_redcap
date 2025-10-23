# Use the official Python base image
FROM python:3.12-slim-bookworm

# Set the working directory for the app
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Run the app
CMD ["python", "flatten.py"]