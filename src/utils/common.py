import os
import io
import re
import logging
import requests
import json
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


def _parse_block_content(content):
    if isinstance(content, dict):
        return content
    try:
        return json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return {}


def _render_delta_text(content_json):
    text = ""
    if 'deltaFormat' in content_json:
        for delta in content_json['deltaFormat']:
            insert = delta.get('insert', '')
            # Если insert - это объект (например, упоминание), пропускаем или обрабатываем отдельно
            if isinstance(insert, dict):
                continue
            segment = str(insert)
            attributes = delta.get('attributes', {})

            if not segment or segment == '\n':
                text += segment
                continue

            if attributes.get('bold'):
                segment = f"**{segment}**"
            if attributes.get('italic'):
                segment = f"*{segment}*"

            styles = []
            if 'color' in attributes:
                styles.append(f"color: {attributes['color']}")
            if 'sanitizedSize' in attributes:
                styles.append(f"font-size: {attributes['sanitizedSize']}")

            if styles:
                style_str = "; ".join(styles)
                segment = f'<span style="{style_str}">{segment}</span>'

            text += segment
    return text


def convert_monday_doc_to_md(blocks):
    """
    Converts a list of Monday Doc blocks into a Markdown string.
    """
    if not blocks:
        return ""

    # Filter blocks that have an id to prevent KeyError
    valid_blocks = [b for b in blocks if b.get('id')]
    block_map = {b['id']: b for b in valid_blocks}
    consumed_ids = set()

    # Identify blocks inside tables to skip them in main iteration
    for block in valid_blocks:
        if block.get('type') == 'table':
            content = _parse_block_content(block.get('content'))
            if 'cells' in content:
                for row in content['cells']:
                    for cell in row:
                        if 'blockId' in cell:
                            consumed_ids.add(cell['blockId'])

    md_lines = []
    for block in valid_blocks:
        if block['id'] in consumed_ids:
            continue

        b_type = block.get('type')
        content_json = _parse_block_content(block.get('content'))

        indent_level = block.get('indentationLevel', 0)
        indent = "    " * indent_level

        if b_type == 'table':
            cells = content_json.get('cells', [])
            if not cells:
                continue

            max_cols = max((len(row) for row in cells), default=0)
            if max_cols == 0:
                continue

            rows_md = []
            for row in cells:
                row_cells = []
                for cell in row:
                    block_id = cell.get('blockId')
                    cell_text = ""
                    if block_id and block_id in block_map:
                        cell_block = block_map[block_id]
                        cell_content = _parse_block_content(
                            cell_block.get('content'))
                        cell_text = _render_delta_text(cell_content)
                        cell_text = cell_text.replace(
                            '\n', '<br>').replace('|', '\\|')
                    row_cells.append(cell_text)

                row_cells.extend([""] * (max_cols - len(row_cells)))
                rows_md.append(f"| {' | '.join(row_cells)} |")

            if rows_md:
                md_lines.append(rows_md[0])
                md_lines.append(f"| {' | '.join(['---'] * max_cols)} |")
                md_lines.extend(rows_md[1:])
        else:
            text = _render_delta_text(content_json)

            if b_type == 'large title':
                md_lines.append(f"{indent}# {text}")
            elif b_type == 'medium title':
                md_lines.append(f"{indent}## {text}")
            elif b_type == 'small title':
                md_lines.append(f"{indent}### {text}")
            elif b_type == 'normal text':
                md_lines.append(f"{indent}{text}")
            elif b_type == 'check list':
                checked = content_json.get('checked', False)
                mark = "x" if checked else " "
                md_lines.append(f"{indent}- [{mark}] {text}")
            elif b_type == 'bulleted list':
                md_lines.append(f"{indent}- {text}")
            elif b_type == 'numbered list':
                md_lines.append(f"{indent}1. {text}")
            elif b_type == 'quote':
                md_lines.append(f"{indent}> {text}")
            elif b_type == 'code':
                md_lines.append(f"{indent}```\n{indent}{text}\n{indent}```")
            elif b_type == 'divider':
                md_lines.append(f"{indent}---")
            elif b_type == 'layout':
                # Layouts are flattened; children are rendered sequentially in the main loop
                pass
            else:
                logging.warning(f"Unhandled block type: {b_type}")

        md_lines.append("")  # Empty line for spacing

    return "\n".join(md_lines)
