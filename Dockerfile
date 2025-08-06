# Use an official Python runtime as a parent images
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy all files from the current directory to the container all
COPY scrap2018papart2/. /app

# Install any needed packages specified in requirements.txt
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    libglib2.0-0 libnss3 libgconf-2-4 libxss1 \
    libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libgtk-3-0 xdg-utils libxcomposite1 libxi6 libxcursor1 \
    libxrandr2 libxdamage1 libdbus-1-3 \
    wget curl unzip gnupg \
    && rm -rf /var/lib/apt/lists/*

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
