import os
import time
import json
import psutil
import config
import uvicorn
import logging
import db_handler


from bson import json_util
from datetime import datetime
from pydantic import BaseModel
from db_handler import db_handler
from utils import clean_output_folders
from contextlib import asynccontextmanager
from files_processing import process_directory
from fastapi import FastAPI, BackgroundTasks, HTTPException
from files_processing import process_pdf_file, process_other_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


app_state = {"is_scan_running": False}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle event handler for FastAPI application"""
    try:
        logger.info("Initializing database connection...")
        await db_handler.initialize()
        logger.info("Testing database connection...")
        connection_test = await db_handler.test_connection()
        logger.info(f"Database connection test result: {connection_test}")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize database connection: {e}")
        raise RuntimeError("Database connection failed")
    finally:
        logger.info("Closing database connection...")
        await db_handler.close()

app = FastAPI(title="File Tracker API", lifespan=lifespan)


class ScanRequest(BaseModel):
    directory: str | None = None

@app.post("/scan", status_code=202)
async def trigger_scan(scan_request: ScanRequest, background_tasks: BackgroundTasks):
    if app_state["is_scan_running"]:
        raise HTTPException(status_code=409, detail="A scan is already in progress.")

    try:
        await db_handler.test_connection()
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection not available")
        
    async def run_scan_and_update_state():
        try:
            app_state["is_scan_running"] = True
            logger.info("Starting background file scan...")
            
            input_dir = scan_request.directory or config.SOURCE_DATA_LAKE_DIR
            if not os.path.exists(input_dir):
                logger.info(f"Directory not found: {input_dir}")
                logger.info("Scan process : ", os.getcwd())
                raise Exception(f"Directory not found: {input_dir}")

            # Clean output directory 
            clean_output_folders(config.OUTPUT_DIR)

            # Process directory and generate report
            report = await process_directory(input_dir)
            
            # Save report to database instead of file
            await db_handler.save_scan_results(report)
            
            logger.info("Scan completed. Results saved to database.")
            
        except Exception as e:
            logger.error(f"Error during background scan: {e}", exc_info=True)
        finally:
            app_state["is_scan_running"] = False

    background_tasks.add_task(run_scan_and_update_state)
    return {"message": "File scan process initiated in the background."}


@app.get("/scan-history")
@app.get("/scan-history/{timestamp}")
async def get_scan_results(timestamp: str = None):
    try:
        collection = db_handler._db[config.RESULTS_COLLECTION]
        if timestamp:
            result = await collection.find_one({"timestamp": timestamp})
            if not result:
                raise HTTPException(status_code=404, detail="Scan results not found")
            return json.loads(json_util.dumps(result))
        else:
            # Return all scan results, sorted by timestamp descending
            cursor = collection.find().sort("timestamp", -1)
            results = await cursor.to_list(length=None)
            return json.loads(json_util.dumps(results))
    except Exception as e:
        logger.error(f"Error retrieving scan results: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving scan results")


@app.get("/last-scan")
async def get_last_scan():
    """Return details of the most recent scan"""
    try:
        collection = db_handler._db[config.RESULTS_COLLECTION]
        # Get the most recent document by sorting on timestamp in descending order
        last_scan = await collection.find_one(
            filter={},
            sort=[("timestamp", -1)]
        )
        
        if not last_scan:
            raise HTTPException(
                status_code=404,
                detail="No scan results found in database"
            )
        
        json_compatible_scan = json.loads(json_util.dumps(last_scan))
        return json_compatible_scan
        
    except Exception as e:
        logger.error(f"Error retrieving last scan results: {e}")
        raise HTTPException(
            status_code=500,
            detail="Error retrieving last scan results"
        )


@app.get("/health")
async def health_check():
    return {
            "status": "running",
            "version": "1.0.0",
            "is_scan_running": app_state["is_scan_running"]
        }
        
if __name__ == "__main__":
    logger.info("Starting Uvicorn server directly...")
    uvicorn.run(app, host="0.0.0.0", port=8080)