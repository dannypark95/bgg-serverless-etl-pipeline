# Use the official, lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and force it to stream logs instantly
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements first (optimizes Docker's build cache)
# Dependencies: google-generativeai, google-cloud-storage, google-cloud-firestore, tqdm
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your automated pipeline scripts into the container
COPY bgg_csv.py .
COPY bgg_extractor.py .
COPY gemini_translator.py .

# By default, we print a success message if the container boots without a specific command.
# Cloud Run Jobs (triggered by Workflows) will override this CMD to run specific scripts.
CMD ["echo", "BGG Pipeline Container ready! Run with 'python bgg_csv.py', 'python bgg_extractor.py', or 'python gemini_translator.py'"]