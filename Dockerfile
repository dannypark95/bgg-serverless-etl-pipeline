# Use the official, lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and force it to stream logs instantly
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements first (this optimizes Docker's build cache)
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your data engineering scripts into the container
COPY bgg_csv.py .
COPY bgg_extractor.py .
COPY boardlife_scraper.py .

# By default, we will just print a success message if the container boots without a specific command.
# Cloud Workflows will override this CMD to run the specific scripts in order.
CMD ["echo", "Container ready! Please specify a script to run (e.g., python bgg_extractor.py)"]