import os
import sys
import time
import utils
import config 
import shutil
import asyncio
import logging
import subprocess
import psutil


from asyncio import Semaphore
from datetime import datetime
from db_handler import db_handler
from config import MAX_CONCURRENT_TASKS
from concurrent.futures import ProcessPoolExecutor
from docling.document_converter import DocumentConverter
from utils import generate_unique_output_path, check_gpu_availability

# docling imports
from pathlib import Path
from docling.datamodel.base_models import InputFormat
from docling.pipeline.simple_pipeline import SimplePipeline
from docling.document_converter import (
    DocumentConverter, 
    PdfFormatOption, 
    WordFormatOption
)
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode


OCR_SEMAPHORE = Semaphore(MAX_CONCURRENT_TASKS)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



async def process_pdf_traditional(original_file_path):
    """
    Process PDF file using docling library with table extraction
    """

    absolute_pdf_path = os.path.abspath(original_file_path)
    logger.info(f"Processing PDF file with docling: {absolute_pdf_path}")

    if "tax" in os.path.dirname(absolute_pdf_path).lower():
        logger.info(f"File is in a TAX folder; skipping OCR and moving file: {absolute_pdf_path}")
        output_file_path = utils.generate_unique_output_path(absolute_pdf_path, config.OUTPUT_DIR)
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        shutil.copy2(absolute_pdf_path, output_file_path)
        
        # Record the operation in the database
        file_data = {
            "directory": os.path.dirname(absolute_pdf_path),
            "file_path": absolute_pdf_path,
            "output_path": output_file_path,
            "status": config.STATUS_PROCESSED,
            "size": os.path.getsize(absolute_pdf_path),
            "processed_date": datetime.utcnow(),
            "ocr_version": "skipped"
        }
        await db_handler.insert_processed_file(file_data)
        return output_file_path

    is_gpu_available = check_gpu_availability()

    try:
        # Configure docling pipeline
        pipeline_options = PdfPipelineOptions(
            do_table_structure=True, 
            do_ocr=True, 
            do_cell_matching=True, 
            use_gpu=True if is_gpu_available else False,
        )
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE

        # Check GPU availabili

        if is_gpu_available:
            logger.info("GPU detected, using GPU acceleration for docling")
        else:
            logger.info("No GPU detected, using CPU for docling processing")

        doc_converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
                # InputFormat.DOCX: WordFormatOption(pipeline_cls=SimplePipeline)
            }
        )

        # Process the document
        start_time = time.time()
        result = doc_converter.convert(absolute_pdf_path)
        duration = time.time() - start_time

        # Generate output path for markdown content
        output_filename = f"{os.path.splitext(os.path.basename(absolute_pdf_path))[0]}.md"
        output_path = os.path.join(config.OUTPUT_DIR, output_filename)
        
        # Save the markdown content
        markdown_content = result.document.export_to_markdown()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        # Prepare file data for database
        file_data = {
            "directory": os.path.dirname(absolute_pdf_path),
            "file_path": absolute_pdf_path,
            "output_path": output_path,
            "status": config.STATUS_PROCESSED,
            "size": os.path.getsize(absolute_pdf_path),
            "processed_date": datetime.utcnow(),
            "ocr_version": "docling_light",
            "processing_time": duration,
            "processor": "docling"
        }

        # Save to database
        await db_handler.insert_processed_file(file_data)
        logger.info(f"Successfully processed file with docling: {absolute_pdf_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error during docling processing: {str(e)}")
        return None

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


async def process_pdf_vlm(original_file_path):
    """
    Process PDF file optimized for H100 GPU
    """
    absolute_pdf_path = os.path.abspath(original_file_path)
    logger.info(f"Processing PDF file: {absolute_pdf_path}")

    if "tax" in os.path.dirname(absolute_pdf_path).lower():
        logger.info(f"File is in a TAX folder; skipping OCR and moving file: {absolute_pdf_path}")
        output_file_path = utils.generate_unique_output_path(absolute_pdf_path, config.OUTPUT_DIR)
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        shutil.copy2(absolute_pdf_path, output_file_path)
        
        # Record the operation in the database
        file_data = {
            "directory": os.path.dirname(absolute_pdf_path),
            "file_path": absolute_pdf_path,
            "output_path": output_file_path,
            "status": config.STATUS_PROCESSED,
            "size": os.path.getsize(absolute_pdf_path),
            "processed_date": datetime.utcnow(),
            "ocr_version": "skipped"
        }
        await db_handler.insert_processed_file(file_data)
        return output_file_path
        

    # Generate output path for markdown file
    # output_filename = f"{os.path.splitext(os.path.basename(absolute_pdf_path))[0]}_output.md"
    # output_path = os.path.join(config.OUTPUT_DIR, output_filename)

    # # Ensure output directory exists
    # os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # Validate input file
    if not os.path.exists(absolute_pdf_path):
        logger.error(f"PDF file not found: {absolute_pdf_path}")
        return None
        
    if os.path.getsize(absolute_pdf_path) == 0:
        logger.error(f"PDF file is empty: {absolute_pdf_path}")
        return None

    async with OCR_SEMAPHORE:
        try:
            logger.info(f"Starting GPU-accelerated PDF processing: {os.path.basename(absolute_pdf_path)}")
            start_time = time.time()
            
            # Use docling CLI command with GPU acceleration
            result = await asyncio.create_subprocess_exec(
                "docling",
                "--pipeline", "vlm",
                "--vlm-model", "smoldocling",
                "--device", "cuda",
                absolute_pdf_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await result.communicate()
            duration = time.time() - start_time
            
            if result.returncode == 0:
                # Save output to markdown file
                # ocr_output = stdout.decode()
                # try:
                #     # with open(output_path, 'w', encoding='utf-8') as f:
                #     #     f.write(ocr_output)
                #     # logger.info(f"Output saved to: {output_path}")
                # except Exception as e:
                #     logger.error(f"Failed to save output file: {str(e)}")
                #     return None

                logger.info(f"GPU-accelerated processing completed in {duration:.2f} seconds")

                # Prepare file data for database
                file_data = {
                    "directory": os.path.dirname(absolute_pdf_path),
                    "file_path": absolute_pdf_path,
                    # "output_path": output_path,
                    "status": config.STATUS_PROCESSED,
                    "size": os.path.getsize(absolute_pdf_path),
                    "processed_date": datetime.utcnow(),
                    "processing_time": duration
                }

                # Retry logic for database insertion
                max_retries = 3
                retry_delay = 5
                for attempt in range(max_retries):
                    try:
                        await db_handler.insert_processed_file(file_data)
                        logger.info(f"Successfully inserted file data for: {absolute_pdf_path}")
                        return absolute_pdf_path
                    except Exception as db_error:
                        if attempt < max_retries - 1:
                            logger.warning(f"Database insertion failed (attempt {attempt + 1}/{max_retries}): {str(db_error)}")
                            await asyncio.sleep(retry_delay)
                        else:
                            logger.error(f"All database insertion attempts failed for: {absolute_pdf_path}")
                            return None
                    
            else:
                error_msg = stderr.decode()
                logger.error(f"GPU-accelerated OCR failed: {error_msg}")
                return None
                
        except Exception as e:
            logger.error(f"Error during GPU-accelerated processing: {str(e)}")
            return None


async def process_other_file(original_file_path):
    try:
        # Check file extension first
        file_extension = os.path.splitext(original_file_path)[1].lower()
        if file_extension not in config.ALLOWED_EXTENSIONS:
            logger.info(f"Skipping file with unsupported extension: {original_file_path}")
            return None

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
        
        # Add record to database with file extension info
        file_data = {
            "directory": os.path.dirname(original_file_path),
            "file_path": original_file_path,
            "output_path": output_file_path,
            "status": config.STATUS_PROCESSED,
            "size": file_size,
            "extension": file_extension,
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
    """Process directory with continuous processing and dynamic CPU/GPU utilization"""
    report = {
        "processed_files": [],
        "skipped_files": [],
        "deleted_files": [],
        "errors": []
    }

    # Check for GPU availability once at the start
    use_gpu = check_gpu_availability()
    
    # Select OCR model based on env variable OCR_MODEL (either "VLM" or "traditional")
    ocr_model = config.OCR_MODEL.lower() if hasattr(config, "OCR_MODEL") else "traditional"
    if ocr_model == "vlm":
        processor_func = process_pdf_vlm
        logger.info("Selected OCR model: VLM")
    else:
        processor_func = process_pdf_traditional
        logger.info("Selected OCR model: traditional")

    logger.info(f"Using {'GPU' if use_gpu else 'CPU'}-based processing with {ocr_model.upper()} model")

    try:
        # Get all current files in directory
        current_files = set()
        pdf_files = []
        other_files = []

        # Scan directory for files
        for root, _, files in os.walk(directory_path):
            for filename in files:
                if not filename.startswith(config.IGNORED_PREFIXES):
                    file_path = os.path.join(root, filename)
                    current_files.add(file_path)
                    if filename.lower().endswith(config.PDF_EXTENSION):
                        pdf_files.append(file_path)
                    else:
                        other_files.append(file_path)

        # Process PDFs with continuous processing
        if pdf_files:
            total_pdfs = len(pdf_files)
            logger.info(f"Found {total_pdfs} PDF files to process")

            # Create a queue for PDF processing
            pdf_queue = asyncio.Queue()
            for pdf in pdf_files:
                await pdf_queue.put(pdf)

            # Track active tasks
            active_tasks = set()
            processed_count = 0

            while not pdf_queue.empty() or active_tasks:
                # If running on CPU, adjust concurrent tasks based on CPU load.
                if not use_gpu:
                    cpu_usage = psutil.cpu_percent(interval=0.1)
                    dynamic_limit = MAX_CONCURRENT_TASKS if cpu_usage < 50 else max(1, int(MAX_CONCURRENT_TASKS * (1 - cpu_usage / 100)))
                    concurrency_limit = dynamic_limit
                    logger.debug(f"CPU usage: {cpu_usage}%, concurrency limit adjusted to: {concurrency_limit}")
                else:
                    concurrency_limit = MAX_CONCURRENT_TASKS

                # Start new tasks if capacity available
                while len(active_tasks) < concurrency_limit and not pdf_queue.empty():
                    pdf_file = await pdf_queue.get()
                    task = asyncio.create_task(processor_func(pdf_file))
                    active_tasks.add(task)

                # Wait for any task to complete
                if active_tasks:
                    done, _ = await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)

                    # Process completed tasks
                    for task in done:
                        active_tasks.remove(task)
                        try:
                            result = await task
                            processed_count += 1

                            if result:
                                report["processed_files"].append({
                                    "path": result,
                                    "output": result
                                })
                                logger.info(f"Processed {processed_count}/{total_pdfs}: {os.path.basename(result)}")
                            else:
                                report["errors"].append({
                                    "path": result if result else "Unknown",
                                    "error": "PDF processing failed"
                                })
                        except Exception as e:
                            logger.error(f"Task failed: {str(e)}")
                            report["errors"].append({
                                "error": str(e)
                            })

                # Check system memory periodically
                memory = psutil.virtual_memory()
                if memory.percent > 85:
                    logger.warning(f"High memory usage ({memory.percent}%). Brief pause...")
                    await asyncio.sleep(5)

        # Process other files
        for file_path in other_files:
            try:
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

        # Handle deleted files
        stored_files = await db_handler.get_all_processed_files()
        stored_file_paths = {file["file_path"] for file in stored_files}
        deleted_files = stored_file_paths - current_files

        for deleted_file in deleted_files:
            await db_handler.update_file_status(deleted_file, config.STATUS_DELETED)
            report["deleted_files"].append({
                "path": deleted_file,
                "reason": "File no longer exists"
            })

    except Exception as e:
        error_msg = f"Directory processing error: {str(e)}"
        report["errors"].append({
            "path": directory_path,
            "error": error_msg
        })
        logger.error(error_msg, exc_info=True)

    logger.info(f"Processing complete. Processed: {len(report['processed_files'])}, Errors: {len(report['errors'])}, Deleted: {len(report['deleted_files'])}")
    return report