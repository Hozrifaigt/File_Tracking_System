import os
import multiprocessing


from dotenv import load_dotenv


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    print(f"Loading environment variables from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    print("Warning: .env file not found. Using default environment variables or expecting them to be set.")

# FOLDERS
SOURCE_DATA_LAKE_DIR = os.getenv("SOURCE_DATA_LAKE_DIR")
OUTPUT_DIR = os.getenv("OUTPUT_DIR") # For non-PDFs

# Database configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "file_tracker_db")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "processed_files")
RESULTS_COLLECTION = os.getenv("RESULTS_COLLECTION", "results")

MAX_CONCURRENT_TASKS = 20

POST_PROCESS_SCRIPT_PATH = os.getenv("POST_PROCESS_SCRIPT_PATH") # I did not change it yet in .env

ALLOWED_EXTENSIONS = {
    '.xlsx', '.xls',
    '.docx', '.doc',
    '.pptx', '.ppt',
    '.txt', '.md',
    '.pdf'
}

STATUS_PROCESSED = "processed"
STATUS_DELETED = "deleted"
STATUS_UPDATED = "updated"
STATUS_REPROCESSED = "reprocessed"
PDF_EXTENSION = '.pdf'
IGNORED_PREFIXES = ('.', '~') # Ignore hidden/temp files
