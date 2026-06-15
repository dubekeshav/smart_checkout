FROM python:3.11-slim

WORKDIR /app

# Installing python depedencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copying application code
COPY . .

# Running the FastAPI app on port 8001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"] 