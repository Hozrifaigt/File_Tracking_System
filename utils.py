import os
import re
import shutil
import config
import logging
import db_handler

from typing import Dict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def sanitize_path_component(component):
    component = component.replace(os.path.sep, '_')
    # Keep alphanumeric, underscore, hyphen, dot. Replace others with underscore.
    sanitized = re.sub(r'[^\w\-\.]+', '_', component)
    sanitized = sanitized.strip('_').strip('-')
    return sanitized if sanitized else "invalid_component"


def generate_unique_output_path(original_file_path, target_base_dir):
    source_base_dir = config.SOURCE_DATA_LAKE_DIR

    try:
        relative_path = os.path.relpath(original_file_path, source_base_dir)
    except ValueError:
        print(f"Warning: File '{original_file_path}' seems outside source base '{source_base_dir}'. Using basename for uniqueness.")
        relative_path = os.path.basename(original_file_path)

    relative_dir, filename = os.path.split(relative_path)
    name, ext = os.path.splitext(filename)

    if relative_dir:
        sanitized_dir_prefix = sanitize_path_component(relative_dir)
        base_filename = f"{sanitized_dir_prefix}_{name}{ext}"
    else:
        base_filename = filename

    # Check for existing files and add counter if needed
    output_path = os.path.join(target_base_dir, base_filename)
    counter = 1
    while os.path.exists(output_path):
        new_filename = f"{name}_{counter}{ext}"
        if relative_dir:
            new_filename = f"{sanitized_dir_prefix}_{new_filename}"
        output_path = os.path.join(target_base_dir, new_filename)
        counter += 1

    return output_path

def get_file_hash(file_path: str) -> str:
    """Calculate file hash for change detection"""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()


def clean_output_folders(output_dir: str) -> None:
    """Remove all contents of the output directory before starting a new scan"""
    try:
        if os.path.exists(output_dir):
            for item in os.listdir(output_dir):
                item_path = os.path.join(output_dir, item)
                if os.path.isfile(item_path):
                    if not item.lower().endswith('.json'):
                        os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            logger.info(f"Cleaned output directory: {output_dir}")
        else:
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
    except Exception as e:
        logger.error(f"Error cleaning output directory {output_dir}: {str(e)}")
        raise