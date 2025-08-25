FROM python:3.10-slim

# Set workdir
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose port
EXPOSE 9001

# Run the app with OTEL instrumentation
CMD ["opentelemetry-instrument", "python3", "app1.py"]
