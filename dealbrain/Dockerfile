# Use the official lightweight Python 3.11 image
FROM python:3.14-slim

# Prevent Python from writing .pyc files and force stdout/stderr to be unbuffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Set the working directory inside the container
WORKDIR /app

# Install uv for ultra-fast dependency resolution
RUN pip install uv

# Install all your required dependencies globally inside the container
# Note: Added 'python-multipart' which FastAPI requires for your File/Form uploads!
RUN uv pip install --system fastapi uvicorn python-multipart python-dotenv asyncpg httpx python-dateutil google-adk google-genai pydantic pdfplumber

# Copy your entire DealBrain application into the container
COPY . /app

# Expose the port Uvicorn will run on
EXPOSE 8080

# Start the FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]