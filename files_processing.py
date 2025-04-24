import os
import sys
import time
import utils
import config 
import shutil
import logging
import subprocess

from datetime import datetime
from db_handler import db_handler
from utils import generate_unique_output_path


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_post_processing(original_pdf_path):
    if not config.POST_PROCESS_SCRIPT_PATH:
        print("  (No post-processing script configured in .env)")
        return True # Indicate success if no script defined

    if not os.path.exists(config.POST_PROCESS_SCRIPT_PATH):
        print(f"  [!] Error: Post-processing script not found at: {config.POST_PROCESS_SCRIPT_PATH}")
        return False

    print(f"  Running post-processing script: {config.POST_PROCESS_SCRIPT_PATH} for {original_pdf_path}")
    try:
        # Example: Pass the original PDF path to the script
        post_process_command = [sys.executable, config.POST_PROCESS_SCRIPT_PATH, "--input-pdf", original_pdf_path]

        print(f"    Command: {' '.join(post_process_command)}")
        result = subprocess.run(post_process_command, capture_output=True, text=True, check=True)
        print(f"    Post-processing script stdout:\n{result.stdout}")
        if result.stderr:
             print(f"    Post-processing script stderr:\n{result.stderr}")
        print(f"  [✓] Post-processing completed successfully.")
        return True

    except FileNotFoundError:
         print(f"  [X] CRITICAL ERROR: Python interpreter or post-process script not found.")
         return False
    except subprocess.CalledProcessError as e:
        print(f"  [!] Error during post-processing script execution (Return Code: {e.returncode}).")
        print(f"    Stdout: {e.stdout}")
        print(f"    Stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"  [X] An unexpected Python error occurred during post-processing: {e}")
        return False


async def process_pdf_file(original_file_path):
    return None
    absolute_pdf_path = os.path.abspath(original_file_path)
    print(f"  Processing PDF: {os.path.basename(absolute_pdf_path)}")

    command_to_run = ["docling", "--pipeline", "vlm", "--vlm-model", "smoldocling", absolute_pdf_path]
    print(f"    Running OCR command: {' '.join(command_to_run)}")

    start_time = time.time()
    ocr_success = False
    try:
        result = subprocess.run(command_to_run, capture_output=False, text=True)
        duration = time.time() - start_time
        if result.returncode == 0:
            print(f"    [✓] Docling OCR successfully processed in {duration:.2f} seconds.")
            ocr_success = True
        else:
            print(f"    [!] Error processing with docling. Code: {result.returncode}. Took {duration:.2f}s")
            ocr_success = False
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n    [X] Unexpected Python error during docling execution for {absolute_pdf_path}: {e}")
        return None

    post_process_success = False
    if ocr_success:
        post_process_success = run_post_processing(absolute_pdf_path)
    else:
        print("    Skipping post-processing due to OCR error.")

    if ocr_success: # and post_process_success:
         print(f"  [✓] PDF processed successfully: {os.path.basename(absolute_pdf_path)}")
         return absolute_pdf_path # Return original path
    else:
         print(f"  [!] PDF processing failed or post-processing failed for: {os.path.basename(absolute_pdf_path)}")
         return None


async def process_other_file(original_file_path):
    try:
        if not os.path.exists(original_file_path):
            logger.error(f"File not found: {original_file_path}")
            return None
            
        output_file_path = generate_unique_output_path(original_file_path, config.OUTPUT_DIR)
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        
        # Add file size check
        file_size = os.path.getsize(original_file_path)
        if file_size == 0:
            logger.warning(f"Empty file detected: {original_file_path}")
            return None
            
        shutil.copy2(original_file_path, output_file_path)
        
        # Add record to database
        file_data = {
            "directory": os.path.dirname(original_file_path),
            "file_path": original_file_path,
            "output_path": output_file_path,
            "status": config.STATUS_PROCESSED,
            "size": file_size,
            "modified": os.path.getmtime(original_file_path),
            "processed_date": datetime.utcnow()
        }
        
        logger.info(f"Attempting to insert file data into database: {file_data}")
        await db_handler.insert_processed_file(file_data)
        logger.info(f"Successfully processed and logged file: {original_file_path}")
        
        return output_file_path
        
    except Exception as e:
        logger.error(f"Error processing file {original_file_path}: {str(e)}", exc_info=True)
        return None

async def process_directory(directory_path: str) -> dict:
    report = {
        "processed_files": [],
        "skipped_files": [],
        "deleted_files": [],
        "errors": []
    }
    
    try:
        # Get all current files in directory
        current_files = set()
        for root, _, files in os.walk(directory_path):
            for filename in files:
                if not filename.startswith(config.IGNORED_PREFIXES):
                    current_files.add(os.path.join(root, filename))

        # Check for deleted files
        stored_files = await db_handler.get_all_processed_files()
        stored_file_paths = {file["file_path"] for file in stored_files}
        
        # Find deleted files (files in DB but not in filesystem)
        deleted_files = stored_file_paths - current_files
        for deleted_file in deleted_files:
            await db_handler.update_file_status(deleted_file, config.STATUS_DELETED)
            report["deleted_files"].append({
                "path": deleted_file,
                "reason": "File no longer exists"
            })

        # Process current files
        for file_path in current_files:
            try:
                # Check if file was previously marked as deleted
                stored_file = await db_handler.get_processed_file(file_path)
                current_size = os.path.getsize(file_path)
                
                if stored_file:
                    if stored_file["status"] == config.STATUS_DELETED:
                        # File was previously deleted but exists now - reprocess it
                        if file_path.lower().endswith(config.PDF_EXTENSION):
                            result = await process_pdf_file(file_path)
                        else:
                            result = await process_other_file(file_path)
                            
                        if result:
                            report["processed_files"].append({
                                "path": file_path,
                                "output": result
                            })
                    elif stored_file["size"] == current_size:
                        # File exists and hasn't changed
                        report["skipped_files"].append({
                            "path": file_path,
                            "reason": "File already processed"
                        })
                        continue
                else:
                    # New file
                    if file_path.lower().endswith(config.PDF_EXTENSION):
                        result = await process_pdf_file(file_path)
                    else:
                        result = await process_other_file(file_path)
                        
                    if result:
                        report["processed_files"].append({
                            "path": file_path,
                            "output": result
                        })
                    
            except Exception as e:
                report["errors"].append({
                    "path": file_path,
                    "error": str(e)
                })
                    
    except Exception as e:
        report["errors"].append({
            "path": directory_path,
            "error": f"Directory processing error: {str(e)}"
        })
        
    return report