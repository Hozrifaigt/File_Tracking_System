# File Tracking System

A FastAPI-based service that tracks and processes files in a specified directory, with MongoDB integration for storing processing results.

## Features

- Real-time file scanning and tracking
- PDF and non-PDF file processing support
- MongoDB integration for persistent storage
- RESTful API endpoints
- Docker containerization
- Background task processing
- Configurable via environment variables

## Prerequisites

- Python 3.12+
- MongoDB
- Docker and Docker Compose (optional)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Hozrifaigt/File_Tracking_System.git
cd chatbot-automate
```

2. Create a `.env` file:
```env
MONGO_URI=mongodb://mongo:27017/file_tracker_db?retryWrites=true&w=majority
DB_NAME=file_tracker_db
COLLECTION_NAME=processed_files
RESULTS_COLLECTION=results

# File System
DATA_LAKE_DIR=<your-data-lake-path>
SOURCE_DATA_LAKE_DIR=/app/data
OUTPUT_DIR=/app/output
POST_PROCESS_DIR=/app/ocred
```

## Docker Deployment

1. Build and run using Docker Compose:
```bash
docker-compose up -d --build
```

2. The service will be available at `http://localhost:8080`

## API Endpoints

- `POST /scan` - Trigger a new file scan
  - Optional body: `{"directory": "custom/path"}`

- `GET /scan-history/{timestamp}` - Get scan results by timestamp
  - Returns all results if no timestamp is provided

- `GET /last-scan` - Get the most recent scan results

- `GET /health` - Check service health status

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| MONGO_URI | MongoDB connection string | mongodb://localhost:27017/ |
| DB_NAME | Database name | file_tracker_db |
| COLLECTION_NAME | Collection for processed files | processed_files |
| RESULTS_COLLECTION | Collection for scan results | results |
| SOURCE_DATA_LAKE_DIR | Source directory to scan | - |
| OUTPUT_DIR | Directory for processed files | - |

## File Processing

- PDF files: Processed using docling OCR
- Non-PDF files: Copied to output directory with metadata tracking
- File changes are detected using MD5 hashing
- Deleted files are tracked in the database

## Database Collections

- `processed_files`: Tracks individual file processing status
- `results`: Stores complete scan results with timestamps

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
