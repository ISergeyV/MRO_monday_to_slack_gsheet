import os
import io
import re
import logging
import requests
from PIL import Image, ImageOps
import config


def download_file(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download file from {url}: {e}")
        return None


def compress_image(file_content, filename, target_size_mb=1.0):
    try:
        if len(file_content) <= target_size_mb * 1024 * 1024:
            return file_content, filename

        img = Image.open(io.BytesIO(file_content))
        img = ImageOps.exif_transpose(img)

        if img.format not in ['JPEG', 'PNG']:
            return file_content, filename

        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')

        output_io = io.BytesIO()
        quality = 85

        img.save(output_io, format='JPEG', quality=quality, optimize=True)

        while output_io.tell() > target_size_mb * 1024 * 1024 and quality > 20:
            output_io.seek(0)
            output_io.truncate()
            quality -= 10
            img.save(output_io, format='JPEG', quality=quality, optimize=True)

        base_name = os.path.splitext(filename)[0]
        new_filename = f"{base_name}.jpg"

        logging.info(
            f"Compressed: {filename} ({len(file_content)/1024/1024:.2f}MB) -> {new_filename} ({output_io.tell()/1024/1024:.2f}MB)")
        return output_io.getvalue(), new_filename

    except Exception as e:
        logging.warning(
            f"Failed to compress {filename}: {e}. Using original.")
        return file_content, filename


def sanitize_filename(name):
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def load_state():
    if os.path.exists(config.STATE_FILE):
        try:
            with open(config.STATE_FILE, 'r') as f:
                val = f.read().strip()
                if val.isdigit():
                    return int(val)
        except Exception as e:
            logging.warning(f"Failed to read state file: {e}")
    return 1


def save_state(item_num):
    try:
        with open(config.STATE_FILE, 'w') as f:
            f.write(str(item_num))
    except Exception as e:
        logging.error(f"Failed to save state: {e}")
