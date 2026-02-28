# Backend Setup and Startup Instructions

## Prerequisites

torch==2.8.0

Before starting, ensure you have the following installed:

- Docker and Docker Compose
- Python 3.x
- pip (Python package installer)

## Installing Kafka in Docker

To set up Kafka using Docker, follow these steps:

1. Ensure Docker is running on your system.
2. Navigate to the `backend` directory.
3. Use Docker Compose to start the Kafka services. This will pull the necessary images and set up the Kafka broker.

> **Optional: Using Aiven Kafka**
>
> - If you prefer a managed Kafka instance, you can log in to Aiven and create a Kafka service.
> - Download the client certificates from the Aiven dashboard and place them in the `backend/certs` folder (e.g. `service.cert`, `service.key`, `ca.pem`).
> - Update the `KAFKA_SERVER_URL` variable in your `.env` (or `.env.example`) to point to the Aiven Kafka URI (e.g. `your-service-name.aivencloud.com:9092`).
> - Make sure your Django settings or Kafka helper code uses SSL and loads the certificates from the `certs` directory.

## Starting the Kafka Server

In one terminal, perform the following steps:

1. Change to the backend directory:

   ```
   cd backend
   ```

2. Start the Kafka services in detached mode:

   ```
   docker compose up -d
   ```

3. Once the services are up, create the `chat-messages` topic:
   ```
   docker exec -it backend-kafka kafka-topics --create --topic chat-messages --bootstrap-server backend-kafka:9092 --partitions 3 --replication-factor 1
   ```

This will start the Kafka server and create the required topic.

## Starting the Django Backend

In a second terminal, perform the following steps:

1. Change to the backend directory:

   ```
   cd backend
   ```

2. Install the required Python packages:

   ```
   pip install -r requirements.txt
   ```

3. Activate the virtual environment (assuming you're using Bash on Windows):

   ```
   source venv/Scripts/activate
   ```

4. Run database migrations:

   ```
   python manage.py makemigrations
   python manage.py migrate
   ```

5. Start the Django development server:
   ```
   python manage.py runserver
   ```

The Django backend should now be running and ready to handle requests.
