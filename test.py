import os
import shutil
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime
from PIL import Image, UnidentifiedImageError
import imagehash
import exifread
import sys
import concurrent.futures
from logging.handlers import RotatingFileHandler
import signal
import contextlib
import threading # 引入 threading 模块用于锁
import re # Import the re module for regular expressions

# ===== 默认配置（可被参数覆盖）=====

HASH_ALGO = 'sha256'
HASH_THRESHOLD = 5
# Keep default for fallback/pattern matching, but will generate numbered files
DEFAULT_LOG_FILE = "photo_dedup.log"
# 添加常见视频格式，但请注意相似度判断仅对图片有效
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.mp4', '.avi', '.mov', '.mkv']
DEFAULT_MIN_SIZE_KB = 100 # 默认最小文件大小为 100 KB
GPS_THRESHOLD = 0.0001
LOG_FILE_SIZE_LIMIT_MB = 10

# ===== 全局中断标志 =====

interrupted = False

# ===== 信号处理函数 =====

def signal_handler(sig, frame):
    """处理 Ctrl+C 中断信号"""
    global interrupted
    print("\n⚠️ 接收到中断信号 (Ctrl+C)。正在尝试安全退出...")
    interrupted = True
    # 在这里不直接退出或暂停，让线程池有机会完成当前任务或响应中断标志

# 捕获 SIGTSTP 信号（Ctrl+Z），实现暂停功能（仅部分系统支持）
def sigtstp_handler(sig, frame):
    """处理 Ctrl+Z (SIGTSTP) 信号，暂停程序"""
    global interrupted
    print("\n⚠️ 程序已暂停。按 Enter 键恢复执行，或按 Ctrl+C 退出...")
    interrupted = True # 标记中断，但后续会通过 input() 或 signal.pause() 阻塞
    # 使用 input() 阻塞主线程，等待用户交互
    input()
    print("▶️ 程序恢复执行...")
    interrupted = False # 重置中断标志，程序继续

# ===== 设置信号处理 =====
# 注意：信号处理主要影响主线程。工作线程的中断需要检查全局中断标志
signal.signal(signal.SIGINT, signal_handler)  # 捕获 Ctrl+C
# signal.signal(signal.SIGTSTP, sigtstp_handler) # 捕获 Ctrl+Z（SIGTSTP），在某些环境下可能干扰线程池

# ===== 工具函数 =====

def file_hash(filepath, algo=HASH_ALGO):
    """计算文件的哈希值"""
    h = hashlib.new(algo)
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logging.error(f"❌ 计算文件哈希失败: {filepath}，原因: {e}")
        return None

def to_decimal_degrees(dms):
    """将 EXIF 的 DMS (度分秒) 格式转换为十进制度数"""
    degrees = float(dms[0].num) / float(dms[0].den)
    minutes = float(dms[1].num) / float(dms[1].den)
    seconds = float(dms[2].num) / float(dms[2].den)
    return degrees + (minutes / 60.0) + (seconds / 3600.0)

def get_gps_coordinates(filepath):
    """从文件 EXIF 中提取 GPS 坐标"""
    try:
        with open(filepath, 'rb') as f:
            try:
                # Process only necessary tags up to GPS info for efficiency
                # Changed stop_tag to be more comprehensive
                tags = exifread.process_file(f, stop_tag="GPS GPSLongitude", details=False)

                # Check if GPS tags are present
                if 'GPS GPSLatitude' in tags and 'GPS GPSLongitude' in tags:
                    try:
                        lat_val = tags['GPS GPSLatitude'].values
                        lon_val = tags['GPS GPSLongitude'].values
                        latitude = to_decimal_degrees(lat_val)
                        longitude = to_decimal_degrees(lon_val)
                        lat_ref = tags.get('GPS GPSLatitudeRef')
                        lon_ref = tags.get('GPS GPSLongitudeRef')

                        if lat_ref and lat_ref.values[0] == 'S':
                            latitude = -latitude
                        if lon_ref and lon_ref.values[0] == 'W':
                            longitude = -longitude
                        return latitude, longitude
                    # Added IndexError to catch potential issues with values list
                    except (AttributeError, TypeError, IndexError) as e:
                        logging.warning(f"⚠️ 解析 GPS 坐标值失败: {filepath}，原因: {e}")
                        return None
                # It's also possible to have one without the other, although less useful
                elif 'GPS GPSLatitude' in tags or 'GPS GPSLongitude' in tags:
                     logging.warning(f"⚠️ GPS 坐标信息不完整 (仅发现Latitude或Longitude): {filepath}")
                     return None
                else:
                    # No GPS tags found
                    return None
            # Catch the specific EXIFError from the submodule
            except exifread.exceptions.EXIFError as e: # <--- CHANGED THIS LINE
                logging.warning(f"⚠️ 读取 EXIF 信息失败 (GPS 相关): {filepath}，原因: {e}")
                return None
    except FileNotFoundError:
        # This might happen if the file is deleted by another thread between finding it and processing
        logging.debug(f"文件未找到，无法读取或处理 GPS 信息: {filepath}") # Use debug level as it's expected in concurrent runs
        return None
    except Exception as e:
        # Catch other potential errors during file open or initial processing
        logging.error(f"❌ 打开文件或处理 GPS 信息失败: {filepath}，原因: {e}")
        return None

def compare_gps(coord1, coord2, threshold=GPS_THRESHOLD):
    """比较两个 GPS 坐标是否在阈值范围内"""
    if coord1 is None or coord2 is None:
        return False
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    return abs(lat1 - lat2) < threshold and abs(lon1 - lon2) < threshold

def is_image_file(filepath):
    """检查文件是否是 PIL 可以打开的图片文件"""
    # Only check common image extensions first for efficiency
    if filepath.suffix.lower() not in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']:
         return False
    try:
        with Image.open(filepath) as img:
            img.verify() # Verify file integrity
        return True
    except Exception: # Catch all exceptions, including UnidentifiedImageError, IOError, etc.
        return False

def get_image_resolution(filepath):
    """获取图片的分辨率 (宽度 * 高度)"""
    if not is_image_file(filepath): # Only try for recognized image files
        return 0
    try:
        with Image.open(filepath) as img:
            return img.size[0] * img.size[1]
    except UnidentifiedImageError as e:
        logging.warning(f"⚠️ 无法识别的图片格式，无法获取分辨率: {filepath}，原因: {e}")
        return 0
    except FileNotFoundError:
        # This might happen if the file is deleted by another thread
        logging.debug(f"文件未找到，无法获取分辨率: {filepath}")
        return 0
    except Exception as e:
        logging.error(f"❌ 获取图片分辨率失败: {filepath}，原因: {e}")
        return 0

def backup_file(file_path, perform_actions, backup_dir, source_dir_arg, simple_backup=False, simple_backup_with_path=False, reason="", overwrite_files=False):
    """备份文件到指定目录"""
    if not perform_actions:
        log_message(f"备份: {file_path} -> {backup_dir} (模拟)", False, False)
        return

    original_name = file_path.stem.replace(" ", "_")
    ext = file_path.suffix.lower()
    # 使用文件的修改时间创建日期目录
    try:
        timestamp_dir_name = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d')
        timestamp_dir = backup_dir / timestamp_dir_name
    except Exception as e:
         logging.error(f"❌ 获取文件修改时间失败: {file_path}，原因: {e}. 使用当前日期代替。")
         timestamp_dir_name = datetime.now().strftime('%Y-%m-%d_error')
         timestamp_dir = backup_dir / timestamp_dir_name


    # 确定备份路径
    target_dir = timestamp_dir
    if simple_backup_with_path:
        # 构建相对于源目录的路径
        try:
            relative_path = file_path.relative_to(source_dir_arg).parent
            target_dir = timestamp_dir / relative_path
        except ValueError:
            # 如果文件不在源目录下 (例如来自可选目录)，则直接放在日期目录下
            logging.warning(f"⚠️ 文件 {file_path} 不在源目录 {source_dir_arg} 下，无法构建相对路径备份。备份到 {timestamp_dir}")
            target_dir = timestamp_dir
        except Exception as e:
            logging.error(f"❌ 构建相对备份路径失败: {file_path}，原因: {e}. 备份到 {timestamp_dir}")
            target_dir = timestamp_dir

    # 确保目标目录存在
    try:
         target_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
         logging.error(f"❌ 创建备份目录失败: {target_dir}，原因: {e}")
         return # 如果目录无法创建，则跳过备份

    # 确定最终文件名
    if simple_backup:
        # 简单模式：原始文件名
        backup_path = target_dir / f"{original_name}{ext}"
        # 在简单模式下，如果同名文件已存在，我们不做特殊处理，让 copy2 决定是否覆盖（取决于 overwrite_files）
        # 如果需要区分同名文件，应使用 -s1 或默认模式
    elif simple_backup_with_path:
        # 原始路径 + 原始文件名
        backup_path = target_dir / f"{original_name}{ext}"
    else:
        # 默认模式：原始文件名 + 原因 + 哈希
        file_hash_name = file_hash(file_path)
        if file_hash_name is None:
             logging.error(f"❌ 无法计算文件哈希，跳过备份: {file_path}")
             return
        suffix = f"_{reason}" if reason else ""
        new_file_name = f"{original_name}{suffix}_{file_hash_name[:8]}{ext}" # 使用哈希前8位避免文件名过长
        backup_path = target_dir / new_file_name

    # 如果备份路径已存在，根据 overwrite_files 选择是否覆盖
    if backup_path.exists():
        if overwrite_files:
            logging.info(f"文件已存在，覆盖: {backup_path}")
        else:
            logging.info(f"备份文件已存在，跳过备份: {backup_path}")
            return

    # 备份文件
    try:
        shutil.copy2(file_path, backup_path)
        log_message(f"已备份: {file_path} → {backup_path}", True, perform_actions)
    except Exception as e:
        logging.error(f"❌ 备份文件失败: {file_path} 到 {backup_path}，原因: {e}")

def log_message(message, enable_console_log, is_executing):
    """根据参数决定是否同时输出日志到控制台"""
    # 日志格式已在 main 中配置，这里只负责调用 logging.info
    logging.info(message)
    # 只有当 enable_console_log 为 True 时才打印到控制台
    # 注意：这里不再使用 print 直接输出，而是依赖于 logging handler
    # 如果 logging 配置中包含了 StreamHandler 并且 enable_console_log 为 True，消息会自动输出到控制台
    pass # Remove direct print here

# Modified log_message to use logging levels and ensure console output via handler
def log_action(level, message, enable_console_log):
    """Log a message with a specific level, optionally printing to console."""
    logger = logging.getLogger() # Get the root logger

    # Check if a console handler is already attached
    has_console_handler = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)

    # If console output is enabled and no console handler exists, add one temporarily
    # This ensures messages are printed even if the main console handler setup is complex
    # However, it's better to rely on the main setup in __main__
    # Simpler approach: just log, and the main setup handles console output
    if level == logging.INFO:
        logging.info(message)
    elif level == logging.WARNING:
        logging.warning(message)
    elif level == logging.ERROR:
        logging.error(message)
    elif level == logging.DEBUG:
        logging.debug(message)
    else:
        logging.info(message) # Default to info

    # No need for explicit print here if StreamHandler is configured in __main__
    # If enable_console_log is True, the StreamHandler added in __main__ will handle it.
    pass


def safe_delete_file(file_path, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=False, simple_backup_with_path=False, reason=""):
    """安全删除文件，可选备份或软删除"""
    # Use log_action for messages that might interfere with progress bar
    if not perform_actions:
        log_action(logging.INFO, f"[删除]: {file_path} (模拟)", enable_console_log)
        return True # 模拟删除成功

    # 在执行删除前，先检查文件是否存在，防止重复删除或删除不存在的文件
    if not file_path.exists():
        log_action(logging.WARNING, f"⚠️ 尝试删除文件 {file_path}，但文件不存在。跳过删除。", enable_console_log)
        return False # 文件不存在，无需删除

    deleted_successfully = False
    if delete_soft and trash_dir:
        # 软删除 (移动到回收站目录)
        trash_path = Path(trash_dir) / file_path.name # 简单地移动到回收站根目录，可能会有文件名冲突
        # 更好的软删除方式是保留部分原目录结构，或者在文件名后加时间戳/哈希
        # 为了简单，这里只移动到根目录，但如果文件名冲突，需要处理（比如加后缀）
        # shutil.move 如果目标存在且不是目录，会覆盖。如果是目录，会移动到目录下。
        # 如果目标路径已存在同名文件，这里会抛出 FileExistsError 或 IsADirectoryError
        try:
            # 如果目标已存在同名文件，加个后缀
            if trash_path.exists():
                name, ext = os.path.splitext(trash_path.name)
                timestamp_suffix = datetime.now().strftime('_%Y%m%d%H%M%S')
                trash_path = trash_path.with_name(f"{name}{timestamp_suffix}{ext}")
                log_action(logging.WARNING, f"⚠️ 回收站已存在同名文件 {file_path.name}，移动到 {trash_path}", enable_console_log)

            # 确保回收站目录存在
            trash_path.parent.mkdir(parents=True, exist_ok=True)

            shutil.move(str(file_path), str(trash_path))
            log_action(logging.INFO, f"[软删除] 已移动到 {trash_path}: {file_path}", enable_console_log)
            deleted_successfully = True
        except Exception as e:
            log_action(logging.ERROR, f"❌ [软删除] 移动文件失败 {file_path} 到 {trash_path}: {e}", enable_console_log)
            deleted_successfully = False # 移动失败，视为删除失败
    else: # 默认行为：先备份再硬删除
        # 备份文件
        # 在执行删除前调用备份
        backup_file(file_path, perform_actions, backup_dir, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason=reason)

        # 执行硬删除
        try:
            file_path.unlink() # 硬删除
            log_action(logging.INFO, f"[删除] (备份已完成): {file_path}", enable_console_log)
            deleted_successfully = True
        except Exception as e:
            log_action(logging.ERROR, f"❌ 硬删除文件失败 {file_path}: {e}", enable_console_log)
            deleted_successfully = False # 删除失败

    return deleted_successfully # 返回是否删除成功

def calculate_phash(filepath, cache, phash_cache_lock):
    """计算图片的感知哈希，带缓存和线程锁"""
    with phash_cache_lock:
        if filepath in cache:
            return cache[filepath]

    # 如果不在缓存中，计算并存入
    phash_val = None
    if not is_image_file(filepath): # 只有图片才能计算phash
         logging.debug(f"跳过文件 {filepath} 的感知哈希计算，因为它不是图片。")
         return None

    try:
        with Image.open(filepath) as img:
            phash_val = imagehash.phash(img.convert('RGB'))
        with phash_cache_lock: # 再次获取锁，确保写入缓存是线程安全的
             cache[filepath] = phash_val
    except UnidentifiedImageError as e:
        logging.warning(f"⚠️ 无法识别的图片格式，无法计算感知哈希: {filepath}，原因: {e}")
    except FileNotFoundError:
        # This might happen if the file is deleted by another thread
        logging.debug(f"文件未找到，无法计算感知哈希: {filepath}")
    except Exception as e:
        logging.error(f"❌ 计算感知哈希失败: {filepath}，原因: {e}")

    return phash_val

# Modified to return whether the *current* file being processed (file) was deleted
# and to handle updating phash_list if the original was deleted
def handle_similar_images(file, original_file, file_phash, original_phash, args, source_dir_arg, phash_list, phash_list_lock):
    """处理相似图片对，根据规则决定删除哪个"""
    # logging.debug(f"比较相似图片: {file} 和 {original_file}") # 这条日志可能过于频繁

    # Check if files still exist
    if not file.exists() or not original_file.exists():
        # One or both files might have been deleted by another thread (e.g. exact duplicate)
        logging.debug(f"相似文件 {file} 或 {original_file} 不存在，跳过相似性比较处理。")
        return False # 当前文件未被删除，也不影响保留计数，因为另一个文件可能已被删除并计入

    perform_actions = args.perform_actions
    backup_dir = Path(args.backup_dir)
    delete_soft = args.delete_soft
    trash_dir = Path(args.trash_dir) if args.trash_dir else None
    enable_console_log = args.log
    simple_backup = args.simple_backup
    simple_backup_with_path = args.simple_backup_path # Use corrected parameter name
    prefer_resolution = args.prefer_resolution

    gps_file = get_gps_coordinates(file)
    gps_orig = get_gps_coordinates(original_file)

    file_deleted = False # Flag to indicate if the current file ('file') was deleted

    # 优先级1: 含有GPS信息
    if gps_file is not None and gps_orig is None:
        # Current file has GPS, original doesn't -> keep current, delete original
        safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps")
        logging.info(f"[相似] 保留含GPS: {file}, 删除: {original_file}")
        # Original was deleted, current was kept. Need to update phash_list later if original was the list entry.
        file_deleted = False

    elif gps_file is None and gps_orig is not None:
        # Original has GPS, current doesn't -> keep original, delete current
        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps")
        logging.info(f"[相似] 保留含GPS: {original_file}, 删除: {file}")
        file_deleted = True # Current file was deleted

    # 优先级2: 比较分辨率或大小 (如果GPS信息相同或都没有)
    else:
        # Both have GPS (and maybe similar location) or neither has GPS
        if prefer_resolution:
            res_file = get_image_resolution(file)
            res_orig = get_image_resolution(original_file)
            size_file = file.stat().st_size if file.exists() else 0 # Check exists before stat
            size_orig = original_file.stat().st_size if original_file.exists() else 0 # Check exists before stat

            if res_file > res_orig:
                # Current file has higher resolution -> keep current, delete original
                safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="resolution")
                logging.info(f"[相似] 保留分辨率更高: {file}, 删除: {original_file}")
                file_deleted = False
            elif res_file < res_orig:
                # Original file has higher resolution -> keep original, delete current
                safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="resolution")
                logging.info(f"[相似] 保留: {original_file} (分辨率更高), 删除: {file}")
                file_deleted = True # Current file was deleted
            else: # Resolution is the same, compare size
                if size_file > size_orig:
                    # Current file is larger -> keep current, delete original
                    safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                    logging.info(f"[相似] 保留文件较大: {file}, 删除: {original_file}")
                    file_deleted = False
                elif size_file < size_orig:
                    # Original file is larger -> keep original, delete current
                    safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                    logging.info(f"[相似] 保留: {original_file} (文件较大), 删除: {file}")
                    file_deleted = True # Current file was deleted
                else:
                    # Resolution and Size are the same, keep the original one encountered first (original_file)
                    safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="similar")
                    logging.info(f"[相似] 保留: {original_file}, 删除: {file} (大小相同)")
                    file_deleted = True # Current file was deleted

        else: # Not preferring resolution, just compare size
            size_file = file.stat().st_size if file.exists() else 0
            size_orig = original_file.stat().st_size if original_file.exists() else 0

            if size_file > size_orig:
                # Current file is larger -> keep current, delete original
                safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                logging.info(f"[相似] 保留文件较大: {file}, 删除: {original_file}")
                file_deleted = False
            elif size_file < size_orig:
                # Original file is larger -> keep original, delete current
                safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                logging.info(f"[相似] 保留: {original_file} (文件较大), 删除: {file}")
                file_deleted = True # Current file was deleted
            else:
                # Size is the same, keep the original one encountered first (original_file)
                safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="similar")
                logging.info(f"[相似] 保留: {original_file}, 删除: {file} (大小相同)")
                file_deleted = True # Current file was deleted

    # After deciding and potentially deleting, update phash_list if the original entry was deleted
    # This part needs the lock
    if not file_deleted: # If the current file was NOT deleted, it means the original_file WAS deleted (or skipped)
         # We should remove the original_file from phash_list and potentially add the current file
         # Add the current file as the representative if it wasn't already in the list
         # This is handled in process_file where it checks if the current file was deleted.
         # If file_deleted is False, process_file will add the current file to phash_list later if needed.
         pass

    # Return True if the CURRENT file being processed was deleted, False otherwise
    return file_deleted

# Modified to return whether the *current* file being processed (file) was deleted
def handle_exact_duplicate(file, original, args, source_dir_arg):
    """处理完全重复的文件对，根据规则决定删除哪个"""
    # logging.debug(f"处理完全重复文件: {file} 和 {original}") # 这条日志可能过于频繁

    # Check if files still exist
    if not file.exists() or not original.exists():
         logging.debug(f"重复文件 {file} 或 {original} 不存在，跳过重复处理。")
         return False # 当前文件未被删除

    # If the file path is exactly the same, it's not a duplicate pair to handle here
    if file == original:
         logging.debug(f"文件 {file} 是自身引用，跳过重复处理。")
         return False # 当前文件未被删除

    perform_actions = args.perform_actions
    backup_dir = Path(args.backup_dir)
    delete_soft = args.delete_soft
    trash_dir = Path(args.trash_dir) if args.trash_dir else None
    enable_console_log = args.log
    simple_backup = args.simple_backup
    simple_backup_with_path = args.simple_backup_path # Use corrected parameter name

    gps_file = get_gps_coordinates(file)
    gps_original = get_gps_coordinates(original)

    file_deleted = False # Flag to indicate if the current file ('file') was deleted

    # 优先级1: 含有GPS信息
    if gps_file is not None and gps_original is None:
        # Current file has GPS, original doesn't -> keep current, delete original
        # In exact duplicates, we usually keep the 'original' one found first (the one in seen_hashes)
        # So, if the 'original' in seen_hashes *doesn't* have GPS but the 'current' one *does*, we should delete the 'original' and update seen_hashes
        # This is complex with threading. A simpler rule is to keep the 'original' in seen_hashes if it has GPS, otherwise delete the current one if it has GPS.
        # Let's stick to the rule: keep the one with GPS. If only current has GPS, keep current, delete original.
        # This is slightly different logic for exact duplicates vs similar duplicates, but reasonable.
         safe_delete_file(original, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps_duplicate")
         logging.info(f"[重复] 保留含GPS: {file}, 删除: {original}")
         file_deleted = False # Original was deleted

    elif gps_file is None and gps_original is not None:
        # Original has GPS, current doesn't -> keep original, delete current
        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps_duplicate")
        logging.info(f"[重复] 保留含GPS: {original}, 删除: {file}")
        file_deleted = True # Current file was deleted

    # 优先级2: 如果GPS信息相同或都没有 -> 默认保留 seen_hashes 中的原文件，删除当前文件
    else:
        # Both have GPS (and possibly same location) or neither has GPS
        # Keep the original one that was recorded first (original in seen_hashes)
        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="duplicate")
        logging.info(f"[重复] 保留: {original}, 删除: {file}")
        file_deleted = True # Current file was deleted

    # Return True if the CURRENT file being processed was deleted, False otherwise
    return file_deleted


# ===== 核心文件处理函数 (在线程中运行) =====
def process_file(file, seen_hashes, phash_cache, phash_list, args, source_dir_arg,
                 seen_hashes_lock, phash_cache_lock, phash_list_lock):
    """处理单个文件，查找重复或相似文件，并根据规则进行操作"""
    global interrupted

    # 检查全局中断标志
    if interrupted:
        logging.info(f"🛑 线程收到中断信号，停止处理文件: {file}")
        return 0 # 因中断跳过，未删除文件

    # 再次检查文件是否存在，防止文件在扫描后和处理前被删除
    if not file.exists():
        logging.debug(f"文件 {file} 不存在，跳过处理。")
        return 0

    try:
        # 检查文件大小
        file_size = file.stat().st_size
        min_size_bytes = args.min_size * 1024
        if file_size < min_size_bytes:
             logging.debug(f"文件 {file} 小于最小扫描大小 ({args.min_size} KB)，跳过。")
             return 0

        # 检查文件扩展名是否在允许范围内
        # 注意：process_directory 已经过滤了一次，但这里可以作为二次确认或针对特定处理步骤的过滤
        if file.suffix.lower() not in IMAGE_EXTENSIONS:
             logging.debug(f"文件 {file} 扩展名不在允许范围 {IMAGE_EXTENSIONS} 内，跳过。")
             # 对于非允许扩展名的文件，不进行任何处理（不删除也不备份）
             return 0


        # 1. 计算文件哈希并检查完全重复文件
        file_hash_val = file_hash(file)
        if file_hash_val is None:
             logging.error(f"❌ 无法计算哈希，跳过文件: {file}")
             return 0 # 哈希计算失败，无法进行任何处理

        is_exact_duplicate = False
        original_exact_file = None
        with seen_hashes_lock:
            if file_hash_val in seen_hashes:
                original_exact_file = seen_hashes[file_hash_val]
                is_exact_duplicate = True
            else:
                # 如果不是完全重复文件（基于内容哈希第一次见），将其哈希添加到 seen_hashes 中
                seen_hashes[file_hash_val] = file

        if is_exact_duplicate:
            # 找到了完全重复文件
            # original_exact_file 是 seen_hashes 中记录的第一个拥有此哈希的文件路径
            if original_exact_file.exists() and file != original_exact_file:
                 # 处理完全重复对，并检查当前文件是否被删除
                 # handle_exact_duplicate 返回 True 如果当前文件被删除
                 file_was_deleted = handle_exact_duplicate(file, original_exact_file, args, source_dir_arg)
                 if file_was_deleted:
                     return 1 # 当前文件被删除了
                 else:
                     # 如果当前文件因为 GPS 等规则被保留了 (原文件被删除)
                     # 理论上它现在是 seen_hashes 中这个哈希对应的更好的文件，但我们不修改 seen_hashes 以简化并发逻辑
                     # 这种情况下当前文件未被删除
                     return 0
            elif file == original_exact_file:
                 # 文件路径完全相同，这是同一个文件，不处理
                 logging.debug(f"文件 {file} 是自身引用，跳过重复检查。")
                 return 0
            else:
                 # original_exact_file 不存在 (可能已被其他线程处理并删除)，那么当前文件也是重复的，但没有原件可以比对。
                 # 这里选择跳过处理当前文件，认为它可能已被其他逻辑处理或不应处理。
                 logging.debug(f"文件 {file} 是哈希重复文件，但原文件 {original_exact_file} 不存在，跳过删除。")
                 return 0 # 未删除当前文件

        # 3. 如果不是完全重复，检查相似图片 (仅对图片且启用相似度检查和去重时)
        # 注意：视频文件不参与相似度检查
        is_img = is_image_file(file)
        # is_video 标志在上方已获取
        can_do_similarity = is_img and args.include_similar and args.deduplicate

        file_was_deleted_as_similar = False # 标记当前文件是否因相似而被删除

        if can_do_similarity:
            # 文件是图片，并且启用了相似度检查和去重
            file_phash = calculate_phash(file, phash_cache, phash_cache_lock)

            if file_phash is not None:
                # 成功计算出感知哈希
                found_similar = False
                with phash_list_lock:
                    # 遍历 phash_list 的副本，避免在迭代时修改同一个列表
                    for original_file, original_phash in list(phash_list):
                         # 检查原文件是否存在且感知哈希有效，并且相似度在阈值内
                         if original_phash is not None and original_file.exists() and abs(file_phash - original_phash) <= args.hash_threshold:
                            found_similar = True
                            logging.debug(f"相似文件 {file} 与 {original_file} 匹配，进行处理...")

                            # 处理相似对，并检查当前文件是否被删除
                            # handle_similar_images 返回 True 如果当前文件被删除，False 如果原文件被删除
                            # handle_similar_images 会负责在锁内更新 phash_list
                            file_was_deleted_as_similar = handle_similar_images(file, original_file, file_phash, original_phash, args, source_dir_arg, phash_list, phash_list_lock)

                            # 找到了相似匹配并处理了，退出相似列表的检查循环
                            break

                # 在检查完 phash_list 后
                if not found_similar:
                    # 如果没有在 phash_list 中找到相似文件
                    # 将当前文件的感知哈希添加到 phash_list 供后续文件比较
                    # 确保是图片且当前文件没有被删除（file_was_deleted_as_similar 为 False）
                    # is_img, args.include_similar, args.deduplicate 此时应为 True
                    with phash_list_lock: # 获取锁来修改 phash_list
                        # 再次检查文件路径是否已经以某种方式被添加到 phash_list（例如被 handle_similar_images 添加）
                        # 尽管 handle_similar_images 应该只在原文件被删除时才添加当前文件，这里多一层检查更保险
                        if not any(f == file for f, _ in phash_list):
                             phash_list.append((file, file_phash))

                # 注意： file_was_deleted_as_similar 标志在 handle_similar_images 中设置

            # --- 这个 elif 处理 can_do_similarity 为 True 但 file_phash 计算失败的情况 ---
            # 它应该与 `if file_phash is not None:` 对齐
            elif is_img: # 此时 can_do_similarity 必为 True
                 logging.warning(f"⚠️ 跳过文件 {file} 的相似性检查，因为感知哈希计算失败。")
                 # 感知哈希计算失败，文件没有因为相似被删除， file_was_deleted_as_similar 保持 False
                 file_was_deleted_as_similar = False # 明确设置，尽管默认是 False

        # --- 这个 else 处理 can_do_similarity 为 False 的情况 ---
        # 它应该与 `if can_do_similarity:` 对齐
        else:
            # 文件不参与相似度比较 (例如：视频文件，或者没有启用相似度检查，或者没有启用去重)
            # 文件没有因为相似被删除， file_was_deleted_as_similar 保持 False
            file_was_deleted_as_similar = False # 明确设置，尽管默认是 False


        # 4. 根据前面的检查结果，决定最终操作 (备份或跳过备份)
        # 如果文件因为精确重复或相似被删除，相应的处理函数已经完成了删除操作并返回了删除标志
        # 如果 file_was_deleted_as_similar 为 True，说明当前文件在相似度检查阶段被删除了
        if file_was_deleted_as_similar:
             # 当前文件因为相似而被删除了
             return 1 # 返回删除计数 1
        # 如果文件没有被删除 (精确重复时被保留，或者不是重复/相似文件，或者相似时被保留)
        # 并且没有设置 --deduplicate-only (即需要备份非重复文件)
        elif not args.deduplicate_only:
            # 备份该文件
            backup_file(file, args.perform_actions, Path(args.backup_dir), args.source_dir, args.simple_backup, args.simple_backup_path, overwrite_files=args.overwrite)
            # 文件未被删除
            return 0
        else: # 文件未被删除，但设置了 --deduplicate-only
            # 不备份非重复文件
            return 0 # 文件未被删除


    except Exception as e:
         logging.error(f"❌ 处理文件 {file} 时发生未知错误: {e}", exc_info=True)
         return 0 # 发生错误，未删除文件

# ... (您的其余代码，包括 process_directory 和 parse_args 保持不变) ...

# ===== 目录扫描和处理函数 =====
# ===== 目录扫描和处理函数 =====
def process_directory(args, source_dir):
    """扫描目录并处理文件"""
    global interrupted # <--- 添加这一行

    # 使用线程安全的字典和列表，并使用锁进行同步
    seen_hashes = {} # {file_hash: first_file_path} 存储文件哈希和第一次遇到的文件路径
    phash_cache = {} # {file_path: phash_value} 存储文件的感知哈希缓存
    phash_list = []  # [(file_path, phash_value)] 存储图片的感知哈希列表，用于相似度比较

    # 创建锁对象
    seen_hashes_lock = threading.Lock()
    phash_cache_lock = threading.Lock()
    phash_list_lock = threading.Lock()


    # 过滤文件
    logging.info(f"🔍 扫描目录: {source_dir}")
    all_files = []
    for f in source_dir.rglob('*'):
        if interrupted:
             logging.info("🛑 扫描目录时收到中断信号，停止扫描。")
             break
        if f.is_file():
            try:
                # 检查文件大小和扩展名
                min_size_bytes = args.min_size * 1024
                # 只有大小符合且扩展名符合的文件才加入待处理列表
                if f.stat().st_size >= min_size_bytes and f.suffix.lower() in IMAGE_EXTENSIONS:
                     # 检查写权限，如果不能写，通常也不能删除或移动
                     if os.access(f, os.W_OK):
                         all_files.append(f)
                     else:
                         logging.warning(f"⚠️ 文件无写入权限，跳过: {f}")
                # else: 文件大小不符合或扩展名不符合，跳过

            except FileNotFoundError:
                logging.debug(f"扫描时文件未找到: {f}") # 可能是文件被删除，正常情况
            except OSError as e:
                 logging.error(f"❌ 扫描文件时发生 OS 错误: {f}，原因: {e}")
            except Exception as e:
                 logging.error(f"❌ 扫描文件时发生未知错误: {f}，原因: {e}")


    scanned_count = len(all_files)
    # deleted_count 和 retained_count 在处理循环中累加或在最后计算
    # deleted_count = 0
    # retained_count = 0

    logging.info(f"共找到 {scanned_count} 个符合条件的文件 (最小大小: {args.min_size} KB, 包含视频: {args.include_videos})")

    # 如果需要检测相似图片并去重，预先计算感知哈希
    # 只有当 --include-similar 和 --deduplicate 同时启用时才进行phash计算
    if args.include_similar and args.deduplicate:
        logging.info("\t🎨 预先计算感知哈希...")
        # 可以使用线程池加速 phash 计算
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            # 使用 partial 将锁和其他不变参数传递给 calculate_phash
            from functools import partial
            # 只有是图片的文件才需要计算 phash
            image_files_for_phash = [f for f in all_files if is_image_file(f)]
            futures = {executor.submit(calculate_phash_partial, file): file for file in image_files_for_phash}

            processed_phash_count = 0
            total_image_files = len(image_files_for_phash)
            print(f"\t🎨 感知哈希计算进度: 0/{total_image_files}", end="", flush=True) # 初始化进度条

            for future in concurrent.futures.as_completed(futures):
                if interrupted:
                     print("\n🛑 预计算感知哈希时收到中断信号。正在尝试关闭线程...")
                     executor.shutdown(wait=False, cancel_futures=True) # 尝试取消剩余任务并立即返回
                     break # 退出结果收集循环
                file = futures[future]
                try:
                    # Get result to catch exceptions in worker threads
                    future.result()
                    processed_phash_count += 1
                    print(f"\t\r🎨 感知哈希计算进度: {processed_phash_count}/{total_image_files}", end="", flush=True)
                except Exception as e:
                    logging.error(f"❌ 预计算文件 {file} 的感知哈希失败: {e}")

            # 如果没有中断，打印完成信息
            if not interrupted:
                 print("\n\t✅ 感知哈希计算完成.")
            else:
                 print("\n\t⚠️ 感知哈希计算被中断.")

        # 构建初始的 phash_list，只包含成功计算出 phash 的图片文件
        with phash_cache_lock: # 获取锁来安全访问 phash_cache
             # 过滤掉 phash 为 None 的项
             phash_list[:] = [(f, h) for f, h in phash_cache.items() if h is not None]
        logging.info(f"✨ 完成感知哈希预计算，共获取到 {len(phash_list)} 个文件的感知哈希用于相似度比较。")


    # 使用线程池处理文件去重和备份
    logging.info(f"🚀 开始处理文件 ({args.threads} 线程)...")
    processed_count = 0
    deleted_in_processing = 0 # Counter for files deleted during processing
    # retained_in_processing = 0 # 可以在循环内计算，无需单独的计数器

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        # 提交所有文件进行处理
        futures = {
            executor.submit(process_file, file, seen_hashes, phash_cache, phash_list, args, source_dir,
                           seen_hashes_lock, phash_cache_lock, phash_list_lock): file
            for file in all_files
        }

        try:
            for future in concurrent.futures.as_completed(futures):
                if interrupted:
                    # 如果在等待 future 完成时收到中断信号，尝试停止执行
                    print("\n🛑 文件处理时收到中断信号。正在等待线程完成当前任务...")
                    break # 退出结果收集循环

                file = futures[future]

                try:
                    # Get the result - it's the number of files deleted by this processing step (either 0 or 1)
                    deleted_by_thread = future.result()
                    deleted_in_processing += deleted_by_thread
                    processed_count += 1

                    # --- 在这里计算当前已处理且未被删除的文件数量 ---
                    retained_so_far = processed_count - deleted_in_processing
                    # --------------------------------------------------

                    # 更新进度条
                    # 将 已保留: {retained_so_far} 加入输出
                    print(f"\t\r📝 处理进度: {processed_count}/{scanned_count}, 已删除: {deleted_in_processing}, 已保留: {retained_so_far}", end="", flush=True)

                    # 心跳日志，每处理一定数量的文件记录一次
                    if processed_count % 100 == 0:
                        logging.info(f"💖 心跳 - 已处理 {processed_count}/{scanned_count} 个文件, 已删除 {deleted_in_processing} 个, 已保留 {retained_so_far} 个") # 日志中也加入保留数

                except concurrent.futures.CancelledError:
                     logging.debug(f"文件 {file} 的处理任务被取消。")
                     processed_count += 1 # 仍然算作一个已处理文件 (跳过处理)
                except Exception as e:
                    logging.error(f"❌ 文件 {file} 的处理线程发生异常: {e}", exc_info=True)
                    processed_count += 1 # 发生错误，仍然算作一个已处理文件 (未删除)

        except KeyboardInterrupt:
             print("\n🛑 脚本主线程捕获到中断信号。正在通知工作线程并等待退出...")
             interrupted = True
             pass # Allow the 'with' block to clean up

    print("\n") # 在进度条完成后打印换行符，确保下一行输出正常

    # 在所有线程完成后计算最终统计
    deleted_count = deleted_in_processing
    # 保留文件总数 = 扫描的文件总数 - 实际被删除的文件总数
    retained_count = scanned_count - deleted_count

    logging.info(f"✅ 目录处理完成: {source_dir}")
    logging.info(f"   扫描文件总数: {scanned_count}")
    logging.info(f"   删除文件总数: {deleted_count}")
    logging.info(f"   保留文件总数: {retained_count}")


    return scanned_count, retained_count, deleted_count


# ===== 命令行参数解析 =====
def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="图片和视频去重备份脚本 (默认模拟执行)")
    parser.add_argument("source_dir", nargs='?', type=str, help="要处理的第一个图片和视频目录 (留空则提示输入)")
    parser.add_argument("--optional-source-dir", nargs='?', type=str, help="可选的第二个图片和视频目录")
    parser.add_argument("backup_dir", nargs='?', type=str, help="被删除文件的备份目录 (留空则提示输入)")
    parser.add_argument("-e", "--execute", dest='perform_actions', action="store_true", help="执行实际的删除和备份操作")
    parser.add_argument("--include-similar", action="store_true", help="启用感知哈希比对，删除相似图片")
    parser.add_argument("-log", action="store_true", help="将日志同时输出到控制台")
    parser.add_argument("--delete-soft", action="store_true", help="启用软删除（移动到指定目录）")
    parser.add_argument("--trash-dir", nargs='?', type=str, help="指定软删除目录 (留空则提示输入)")
    parser.add_argument("--log-dir", nargs='?', type=str, help="指定日志文件输出目录 (留空则提示输入)")
    parser.add_argument("--hash-threshold", type=int, default=HASH_THRESHOLD, help=f"相似图片哈希值阈值 (默认: {HASH_THRESHOLD})")
    parser.add_argument("--threads", type=int, default=4, help="设置处理线程数 (默认: 4)")
    parser.add_argument("--prefer-resolution", action="store_true", help="对于相似图片，优先保留分辨率更高的版本")
    parser.add_argument("-m", "--min-size", type=int, default=DEFAULT_MIN_SIZE_KB, help=f"设置最小扫描文件大小 (KB, 默认: {DEFAULT_MIN_SIZE_KB} KB)")
    parser.add_argument("-v", "--include-videos", action="store_true", help="包含视频文件进行检测 (支持 .mp4, .avi, .mov, .mkv)")
    parser.add_argument("-s", "--simple-backup", action="store_true", help="启用简单备份模式：仅保留原始文件名，相同文件名时按源文件路径备份")
    parser.add_argument("-s1", "--simple-backup-path", action="store_true", help="启用备份模式：原始路径+原始文件名") # 修正参数名为 simple-backup-path
    parser.add_argument("--overwrite", action="store_true", help="覆盖现有备份文件 (注意：不覆盖整个目录)") # 明确说明不覆盖整个目录
    parser.add_argument("--deduplicate", action="store_true", help="启用重复文件查找和删除 (包括精确重复和相似图片，如果启用相似度)")
    parser.add_argument("--deduplicate-only", action="store_true", help="只查找重复文件并删除，不备份非重复文件")
    return parser.parse_args()

# ===== 主程序入口 =====
if __name__ == "__main__":
    args = parse_args()

    source_directories = []

    # 确保至少有一个源目录
    if not args.source_dir:
        args.source_dir = input("\n\t请输入要处理的第一个图片和视频目录: ")
    source_directory_1 = Path(args.source_dir)
    if not source_directory_1.exists():
         print(f"❌ 错误: 源目录不存在: {source_directory_1}")
         sys.exit(1)
    source_directories.append(source_directory_1)


    if args.optional_source_dir:
        source_directory_2 = Path(args.optional_source_dir)
        if not source_directory_2.exists():
             print(f"❌ 错误: 可选源目录不存在: {source_directory_2}")
        else:
             source_directories.append(source_directory_2)

    # 确保备份目录存在且可写
    if not args.backup_dir:
        args.backup_dir = input("\t请输入被删除文件的备份目录: ")
    backup_directory = Path(args.backup_dir)
    try:
        backup_directory.mkdir(parents=True, exist_ok=True)
    except Exception as e:
         print(f"❌ 错误: 无法创建或访问备份目录 {backup_directory}，原因: {e}")
         sys.exit(1)


    perform_actions = args.perform_actions
    include_similar = args.include_similar
    enable_console_log = args.log
    delete_soft = args.delete_soft
    num_threads = args.threads
    prefer_resolution = args.prefer_resolution
    min_size_kb = args.min_size
    include_videos = args.include_videos
    simple_backup = args.simple_backup
    simple_backup_with_path = args.simple_backup_path # 使用修正后的参数名
    overwrite_files = args.overwrite
    deduplicate = args.deduplicate
    deduplicate_only = args.deduplicate_only

    trash_dir = args.trash_dir
    if delete_soft and not trash_dir:
        trash_dir = input("\t请输入软删除目录: ")
    trash_directory = Path(trash_dir) if trash_dir else None
    if delete_soft and trash_directory and not trash_directory.exists():
         try:
             trash_directory.mkdir(parents=True, exist_ok=True)
         except Exception as e:
              print(f"❌ 错误: 无法创建或访问软删除目录 {trash_directory}，原因: {e}")
              # 如果软删除目录无法创建，禁用软删除
              logging.error(f"❌ 无法创建软删除目录 {trash_directory}，禁用软删除。原因: {e}")
              delete_soft = False
              trash_directory = None


    # --- Log file naming modification starts here ---
    log_dir = args.log_dir
    log_base_name = "photo_dedup"
    log_extension = ".log"
    log_parent_dir = None

    if log_dir:
        log_parent_dir = Path(log_dir)
        try:
            log_parent_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"❌ 错误: 无法创建或访问日志目录 {log_parent_dir}，原因: {e}. 将日志写入备份目录。")
            # Fallback to backup directory for logs if specified log_dir fails
            log_parent_dir = backup_directory
            try:
                log_parent_dir.mkdir(parents=True, exist_ok=True) # Ensure backup dir is writable
            except Exception as e_backup:
                print(f"❌ 错误: 无法创建或访问备份目录 {log_parent_dir} 以写入日志，原因: {e_backup}. 脚本将退出。")
                sys.exit(1) # Exit if cannot write logs to backup dir either
    else:
        # If no log_dir is specified, use the backup directory
        log_parent_dir = backup_directory
        try:
            log_parent_dir.mkdir(parents=True, exist_ok=True) # Ensure backup dir is writable
        except Exception as e:
            print(f"❌ 错误: 无法创建或访问备份目录 {log_parent_dir} 以写入日志，原因: {e}. 脚本将退出。")
            sys.exit(1) # Exit if cannot write logs to backup dir

    # Find the next available log file number
    max_num = 0
    # Regex to find "photo_dedup" followed by digits, ending with ".log"
    # Also handle the base name "photo_dedup.log"
    log_file_pattern = re.compile(rf"^{re.escape(log_base_name)}(\d+)?{re.escape(log_extension)}$")

    try:
        for existing_file in log_parent_dir.iterdir():
            match = log_file_pattern.match(existing_file.name)
            if match:
                # Group 1 contains the number part, if present
                num_str = match.group(1)
                if num_str:
                    try:
                        max_num = max(max_num, int(num_str))
                    except ValueError:
                        # Should not happen with the regex, but handle defensively
                        logging.warning(f"⚠️ 发现异常日志文件名，无法解析编号: {existing_file.name}")
                else:
                    # This matches the base name "photo_dedup.log" without a number.
                    # We can treat this as number 0 or 1, depending on desired behavior.
                    # To ensure numbering starts from 1 if base exists, treat base as 1
                    # or just find max of numbered ones and add 1.
                    # Let's find max of numbered ones and add 1. If no numbered ones, start with 1.
                    # So if "photo_dedup.log" exists but no numbered ones, max_num remains 0, new num is 1. Correct.
                    pass

    except Exception as e:
         print(f"❌ 错误: 扫描日志目录 {log_parent_dir} 时发生错误，原因: {e}. 将使用默认文件名。")
         # Fallback to default naming if scanning fails
         log_file_path = log_parent_dir / DEFAULT_LOG_FILE
         # Configure basic logging handler to report this error
         logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
         logging.error(f"❌ 扫描日志目录 {log_parent_dir} 时发生错误，原因: {e}. 将使用默认文件名 {log_file_path}.")

    # Determine the new log file name with the incremented number
    new_log_number = max_num + 1
    log_file_name = f"{log_base_name}{new_log_number}{log_extension}"
    log_file_path = log_parent_dir / log_file_name
    # --- Log file naming modification ends here ---


    # Configure more robust logging
    # Ensure the parent directory for the log file exists (already done above, but double check)
    try:
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"❌ 错误: 无法创建日志文件目录 {log_file_path.parent}，原因: {e}. 请手动创建或检查权限。")
        sys.exit(1) # Exit if log directory cannot be created

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # maxBytes set in bytes
    log_handler = RotatingFileHandler(log_file_path, maxBytes=LOG_FILE_SIZE_LIMIT_MB * 1024 * 1024, backupCount=3, encoding='utf-8') # Specify encoding
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger()
    # Set INFO or DEBUG level based on -log parameter, DEBUG for more detailed debug info
    logger.setLevel(logging.DEBUG if args.log else logging.INFO)
    # Clear potentially existing handlers to prevent duplicates
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(log_handler)

    if enable_console_log:
        # Only add StreamHandler if console logging is enabled and no StreamHandler exists
        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
             console_handler = logging.StreamHandler(sys.stdout)
             console_handler.setFormatter(log_formatter)
             logger.addHandler(console_handler)


    logging.info("==== 脚本开始运行 ====")
    logging.info(f"源目录: {source_directories}")
    logging.info(f"备份目录: {backup_directory}")
    logging.info(f"运行模式: {'执行' if perform_actions else '模拟'}")
    logging.info(f"启用重复文件查找和删除 (--deduplicate): {deduplicate}")
    logging.info(f"仅查找重复文件并删除 (--deduplicate-only): {deduplicate_only}")
    if deduplicate:
        logging.info(f"包含相似图片 (--include-similar): {include_similar}")
        if include_similar:
            logging.info(f"相似度哈希阈值 (--hash-threshold): {args.hash_threshold}")
            logging.info(f"优先保留高分辨率 (--prefer-resolution): {prefer_resolution}")
    logging.info(f"最小扫描文件大小 (-m): {min_size_kb} KB")
    logging.info(f"包含视频文件 (-v): {include_videos}")
    logging.info(f"简单备份模式 (-s): {simple_backup}")
    logging.info(f"原始路径备份模式 (-s1): {simple_backup_with_path}")
    logging.info(f"覆盖现有备份 (--overwrite): {overwrite_files}")
    logging.info(f"软删除 (--delete-soft): {delete_soft}")
    if delete_soft:
        logging.info(f"回收站目录 (--trash-dir): {trash_directory}")
    logging.info(f"使用线程数 (--threads): {num_threads}")
    logging.info(f"日志文件: {log_file_path}") # Log the actual file name being used
    logging.info(f"日志输出到控制台 (-log): {enable_console_log}")

    # === 您提供的控制台输出代码 ===
    print(f"\t运行模式: {'执行' if perform_actions else '模拟'}")
    print(f"\t包含相似图片: {include_similar}")
    print(f"\t相似度哈希阈值: {args.hash_threshold}")
    print(f"\t使用线程数: {num_threads}")
    print(f"\t优先保留高分辨率: {prefer_resolution}")
    print(f"\t最小扫描文件大小: {min_size_kb} KB")
    print(f"\t同时输出日志到控制台: {enable_console_log}")
    print(f"\t包含视频文件: {include_videos}")
    # Determine and print the active backup mode
    active_backup_mode_print = "默认"
    if simple_backup:
        active_backup_mode_print = "简单(-s)"
    elif simple_backup_with_path:
        active_backup_mode_print = "原始路径(-s1)"
    print(f"\t备份模式: {active_backup_mode_print}") # 改为合并输出
    print(f"\t软删除: {delete_soft}")
    if delete_soft:
        print(f"\t回收站目录: {trash_directory}")
    # =============================

    sys.stdout.flush() # 添加这一行

    all_scanned_count = 0
    all_retained_count = 0
    all_deleted_count = 0

    try:
        for source_dir in source_directories:
            source_path = Path(source_dir)
            # 在 process_directory 中已经检查目录存在性，这里不再重复检查
            scanned_count, retained_count, deleted_count = process_directory(
                args=args,
                source_dir=source_path,
            )
            all_scanned_count += scanned_count
            all_retained_count += retained_count
            all_deleted_count += deleted_count
    except Exception as e:
         logging.critical(f"脚本主循环发生致命错误: {e}", exc_info=True)
         print(f"\n❌ 脚本运行中断，发生致命错误: {e}")
    except KeyboardInterrupt:
         # 已在 signal_handler 中处理，这里捕获是为了防止意外
         print("\n🛑 脚本被用户中断。")
         logging.info("==== 脚本被用户中断 ====")
    finally:
        # 最终统计
        logging.info("\t==== 脚本运行结束 ====")
        logging.info(f"\t所有源目录统计：")
        logging.info(f"\t   扫描文件总数: {all_scanned_count}")
        logging.info(f"\t   删除文件总数: {all_deleted_count}")
        logging.info(f"\t   保留文件总数: {all_retained_count}")
        print(f"\n==== 脚本运行结束 ====")
        print(f"所有源目录统计：")
        print(f"\t   扫描文件总数: {all_scanned_count}")
        print(f"\t   删除文件总数: {all_deleted_count}")
        print(f"\t   保留文件总数: {all_retained_count}\n")
