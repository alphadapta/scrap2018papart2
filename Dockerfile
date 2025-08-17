# Use an official Python runtime as a parent images
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy all files from the current directory to the container all
COPY scrap2018papart2/. /app



# Install Python packages
RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    pandas \
    certifi \
    pytz \
    selenium \
    urllib3

# If get_link.py is located in the same directory, uncomment the following line
# COPY get_link.py /app

# Define environment variable
ENV PYTHONUNBUFFERED 1

# Run the Python script
CMD ["python", "main.py"]
