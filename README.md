# DSCI-551-Project-ChatDB_4

A travel information system that combines flight and hotel data with natural language processing capabilities.

## System Architecture

This project consists of several components:

- **Backend API**: A FastAPI application that handles requests for flight and hotel data
- **Frontend**: A Gradio interface that provides user-friendly access to the data
- **MongoDB**: Stores flight information
- **SQLite**: Stores hotel review information
- **Ollama**: Runs a local LLM (Large Language Model) for natural language processing

## Features

- Search for flights by airports or airlines
- Search for hotel reviews by county or state
- Natural language interface for querying both datasets
- Docker-based deployment for easy setup and scalability

## Prerequisites

- Docker and Docker Compose
- At least 8GB of RAM (for running the LLM)
- Approximately 10GB of disk space

## Setup Instructions

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/DSCI-551-Project-ChatDB_4.git
   cd DSCI-551-Project-ChatDB_4
   ```

2. Make sure you have the hotel.db SQLite database in the data directory:
   ```bash
   mkdir -p data
   # Copy your hotel.db into the data directory
   ```

3. Start the application with Docker Compose:
   ```bash
   docker-compose up -d
   ```

4. Wait for all services to start (this might take a few minutes on first run as the Ollama model is downloaded):
   ```bash
   docker-compose logs -f
   ```

5. Access the Gradio frontend at: http://localhost:7860

## Using the Application

### Natural Language Interface

Type queries like:
- "Show me all flights from LAX to JFK"
- "What are the hotels in Orange County, California?"
- "Show me all Delta Airlines flights"

### Manual Search Interface

Use the dedicated tabs for more structured searches:
- **Flights Search**: Filter by airports or airlines
- **Hotels Search**: Filter by county or state

## Development

### Project Structure

```
DSCI-551-Project-ChatDB_4/
│
├── app/                           # Backend application package
│   ├── __init__.py                # Makes 'app' a Python package
│   ├── main.py                    # FastAPI backend entry point
│   ├── db_config.py               # Database configuration settings
│   ├── mongo_agent.py             # MongoDB interaction functions
│   └── sql_agent.py               # SQLite interaction functions
│
├── data/                          # Directory for data files
│   └── hotel.db                   # SQLite database for hotel data
│
├── docker-compose.yml             # Docker Compose configuration
├── Dockerfile                     # Backend Docker configuration
├── Dockerfile.gradio              # Frontend Docker configuration
├── frontend-requirements.txt      # Frontend dependencies
├── gradio_app.py                  # Standalone Gradio application
├── requirements.txt               # Backend dependencies
└── README.md                      # Project documentation
```

### Environment Variables

The following environment variables can be customized:

- `API_URL`: URL of the backend API (default: http://backend:8000)
- `OLLAMA_HOST`: URL of the Ollama service (default: http://ollama:11434)
- `MONGO_URI`: MongoDB connection string (default: MongoDB Atlas connection)
- `MONGO_HOST`: MongoDB hostname (default: mongodb)
- `MONGO_PORT`: MongoDB port (default: 27017)
- `SQLITE_DB_PATH`: Path to the SQLite database (default: ./data/hotel.db)

## Troubleshooting

### MongoDB Connection Issues

If you encounter issues connecting to MongoDB, check the following:
- Ensure your MongoDB connection string in `db_config.py` is correct
- Verify the MongoDB container is running: `docker-compose ps mongodb`
- Check MongoDB logs: `docker-compose logs mongodb`

### Ollama Model Loading Issues

If the Ollama model fails to load:
- Increase the memory allocated to Docker
- Check Ollama logs: `docker-compose logs ollama`
- Try restarting the Ollama container: `docker-compose restart ollama`

### API Connection Errors

If the frontend can't connect to the API:
- Ensure all containers are running: `docker-compose ps`
- Check the API logs: `docker-compose logs backend`
- Verify that the `API_URL` environment variable is set correctly in the frontend container
