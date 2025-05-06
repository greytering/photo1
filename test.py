import os
import shutil
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime
from PIL import Image
import imagehash
import exifread
import sys
import concurrent.futures

# ===== 默认配置（可被参数覆盖）=====
HASH_ALGO = 'sha256'
HASH_THRESHOLD = 5
DEFAULT_LOG_FILE = "photo_dedup.log"  # 保留默认文件名
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.mp4', '.avi', '.mov', '.mkv'] # 添加常见视频格式
DEFAULT_MIN_SIZE_KB = 100 # 默认最小文件大小为 100 KB
GPS_THRESHOLD = 0.0001

# ===== 工具函数 =====

def file_hash(filepath, algo=HASH_ALGO):
    h = hashlib.new(algo)
    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def to_decimal_degrees(dms):
    degrees = float(dms[0].num) / float(dms[0].den)
    minutes = float(dms[1].num) / float(dms[1].den)
    seconds = float(dms[2].num) / float(dms[2].den)
    return degrees + (minutes / 60.0) + (seconds / 3600.0)

def get_gps_coordinates(filepath):
    try:
        with open(filepath, 'rb') as f:
            tags = exifread.process_file(f, stop_tag="GPS GPSLatitude", details=False)
            if 'GPS GPSLatitude' in tags and 'GPS GPSLongitude' in tags:
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
        return None
    except Exception:
        return None

def compare_gps(coord1, coord2, threshold=GPS_THRESHOLD):
    if coord1 is None or coord2 is None:
        return False
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    return abs(lat1 - lat2) < threshold and abs(lon1 - lon2) < threshold

def are_similar_images(file1, file2, threshold=HASH_THRESHOLD):
    try:
        hash1 = imagehash.phash(Image.open(file1).convert('RGB'))
        hash2 = imagehash.phash(Image.open(file2).convert('RGB'))
        return abs(hash1 - hash2) <= threshold
    except Exception:
        return False

def get_image_resolution(filepath):
    try:
        img = Image.open(filepath)
        return img.size[0] * img.size[1]
    except Exception:
        return 0

def backup_file(file_path, perform_actions, backup_dir, source_dir_arg, simple_backup=False, simple_backup_with_path=False, reason=""):
    original_name = file_path.stem.replace(" ", "_")
    ext = file_path.suffix.lower()
    timestamp_dir = backup_dir / datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d')
    timestamp_dir.mkdir(parents=True, exist_ok=True)

    if simple_backup_with_path:
        parts = file_path.parts
        relative_path_parts = parts[len(Path(source_dir_arg).parts):-1]
        recursive_backup_dir = timestamp_dir / Path(*relative_path_parts)
        recursive_backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = recursive_backup_dir / f"{original_name}{ext}"
    elif simple_backup:
        backup_path = timestamp_dir / f"{original_name}{ext}"
        if backup_path.exists():
            parts = file_path.parts
            relative_path_parts = parts[len(Path(source_dir_arg).parts):-1] # 获取源目录后的相对路径
            recursive_backup_dir = timestamp_dir / Path(*relative_path_parts)
            recursive_backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = recursive_backup_dir / f"{original_name}{ext}"
    else:
        file_hash_name = file_hash(file_path)
        suffix = f"_{reason}" if reason else ""
        new_file_name = f"{original_name}{suffix}_{file_hash_name}{ext}"
        backup_path = timestamp_dir / new_file_name
        if len(str(backup_path)) > 255 and hasattr(args, 'source_dir'): # 处理长文件名
            source_path = Path(args.source_dir)
            parts = file_path.parts
            relative_path_parts = parts[len(source_path.parts):-1]
            recursive_backup_dir = timestamp_dir / Path(*relative_path_parts)
            recursive_backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = recursive_backup_dir / new_file_name

    backup_path.parent.mkdir(parents=True, exist_ok=True)

    if backup_path.exists():
        logging.info(f"备份文件已存在，跳过: {backup_path}")
        return

    if perform_actions:
        shutil.copy2(file_path, backup_path)
    logging.info(f"{'[执行] ' if perform_actions else '[模拟] '}已备份: {file_path} → {backup_path}")

def is_image_file(filepath):
    try:
        Image.open(filepath).verify()
        return True
    except Exception:
        return False

def log_message(message, enable_console_log, is_executing):
    log_prefix = "[执行] " if is_executing else "[模拟] "
    logging.info(log_prefix + message)
    if enable_console_log:
        print(log_prefix + message)

def safe_delete_file(file_path, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, is_executing, source_dir_arg, simple_backup=False, simple_backup_with_path=False, reason=""):
    if not perform_actions:
        log_message(f"[删除]: {file_path}", enable_console_log, is_executing)
        return

    if delete_soft and trash_dir:
        trash_path = Path(trash_dir) / file_path.name
        trash_path.parent.mkdir(parents=True, exist_ok=True)
        backup_file(file_path, perform_actions, backup_dir, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason=f"backup_before_soft_delete_{reason}")
        try:
            shutil.move(str(file_path), str(trash_path))
            log_message(f"[软删除] 已移动到 {trash_path}: {file_path}", enable_console_log, is_executing)
        except Exception as e:
            logging.error(f"❌ [软删除] 移动文件失败 {file_path} 到 {trash_path}: {e}")
    else: # 默认行为仍然是备份后删除
        backup_file(file_path, perform_actions, backup_dir, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason=reason)
        try:
            file_path.unlink()
            log_message(f"[删除] (备份已完成): {file_path}", enable_console_log, is_executing)
        except Exception as e:
            logging.error(f"❌ 删除文件失败 {file_path}: {e}")

def calculate_phash(filepath, cache):
    if filepath not in cache:
        try:
            cache[filepath] = imagehash.phash(Image.open(filepath).convert('RGB'))
        except Exception as e:
            logging.warning(f"\t感知哈希计算失败: {filepath}，原因: {e}")
            return None
    return cache[filepath]

def handle_exact_duplicate(file, original, args, source_dir_arg):
    gps_file = get_gps_coordinates(file)
    gps_original = get_gps_coordinates(original)
    perform_actions = args.d
    backup_dir = Path(args.backup_dir)
    delete_soft = args.delete_soft
    trash_dir = Path(args.trash_dir) if args.trash_dir else None
    enable_console_log = args.log
    simple_backup = args.simple_backup
    simple_backup_with_path = args.s1

    if gps_file is not None and gps_original is None:
        safe_delete_file(original, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps")
        log_message(f"保留含GPS: {file}, 删除: {original}", enable_console_log, perform_actions)
        return 1, 1
    elif gps_file is None and gps_original is not None:
        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps")
        log_message(f"保留含GPS: {original}, 删除: {file}", enable_console_log, perform_actions)
        return 1, 0
    elif gps_file is not None and gps_original is not None and compare_gps(gps_file, gps_original):
        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="duplicate")
        log_message(f"保留: {original}, 删除: {file}", enable_console_log, perform_actions)
        return 1, 0
    else:
        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="duplicate") # 如果 GPS 信息差异过大，暂时视为重复文件
        log_message(f"保留: {original}, 删除: {file} (GPS位置可能不同)", enable_console_log, perform_actions)
        return 1, 0

def handle_new_file(file, seen_hashes, phash_cache, phash_list, args, source_dir_arg):
    is_duplicate = False
    perform_actions = args.d
    backup_dir = Path(args.backup_dir)
    delete_soft = args.delete_soft
    trash_dir = Path(args.trash_dir) if args.trash_dir else None
    enable_console_log = args.log
    simple_backup = args.simple_backup
    simple_backup_with_path = args.s1
    hash_threshold = args.hash_threshold
    prefer_resolution = args.prefer_resolution

    if args.include_similar:
        file_phash = calculate_phash(file, phash_cache)
        if file_phash is not None:
            for original_file, original_phash in list(phash_list): # 使用 list 进行安全迭代
                if original_phash is not None and abs(file_phash - original_phash) <= hash_threshold:
                    gps_file = get_gps_coordinates(file)
                    gps_orig = get_gps_coordinates(original_file)

                    if gps_file and not gps_orig:
                        safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps")
                        log_message(f"[相似] 保留含GPS: {file}, 删除: {original_file}", enable_console_log, perform_actions)
                        phash_list[:] = [(f, h) for f, h in phash_list if f != original_file]
                        phash_list.append((file, file_phash))
                        return 1, 1
                    elif not gps_file and gps_orig:
                        safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="gps")
                        log_message(f"[相似] 保留含GPS: {original_file}, 删除: {file}", enable_console_log, perform_actions)
                        return 1, 0
                    else:
                        if prefer_resolution:
                            res_file = get_image_resolution(file)
                            res_orig = get_image_resolution(original_file)
                            if res_file > res_orig:
                                safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="resolution")
                                log_message(f"[相似] 保留分辨率更高: {file}, 删除: {original_file}", enable_console_log, perform_actions)
                                phash_list[:] = [(f, h) for f, h in phash_list if f != original_file]
                                phash_list.append((file, file_phash))
                                return 1, 1
                            elif res_file < res_orig:
                                safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="resolution")
                                log_message(f"[相似] 保留: {original_file} (分辨率更高), 删除: {file}", enable_console_log, perform_actions)
                                return 1, 0
                            else: # 如果分辨率相同，则比较文件大小
                                if file.stat().st_size > original_file.stat().st_size:
                                    safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                                    log_message(f"[相似] 保留文件较大: {file}, 删除: {original_file}", enable_console_log, perform_actions)
                                    phash_list[:] = [(f, h) for f, h in phash_list if f != original_file]
                                    phash_list.append((file, file_phash))
                                    return 1, 1
                                else:
                                    safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                                    log_message(f"[相似] 保留: {original_file} (文件较大), 删除: {file}", enable_console_log, perform_actions)
                                    return 1, 0
                        else: # 如果不优先考虑分辨率，则比较文件大小
                            if file.stat().st_size > original_file.stat().st_size:
                                safe_delete_file(original_file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                                log_message(f"[相似] 保留文件较大: {file}, 删除: {original_file}", enable_console_log, perform_actions)
                                phash_list[:] = [(f, h) for f, h in phash_list if f != original_file]
                                phash_list.append((file, file_phash))
                                return 1, 1
                            else:
                                safe_delete_file(file, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, perform_actions, source_dir_arg, simple_backup=simple_backup, simple_backup_with_path=simple_backup_with_path, reason="larger")
                                log_message(f"[相似] 保留: {original_file} (文件较大), 删除: {file}", enable_console_log, perform_actions)
                                return 1, 0
                    is_duplicate = True
                    break
            if not is_duplicate and file_phash is not None:
                phash_list.append((file, file_phash))

    if not is_duplicate:
        seen_hashes[content_hash] = file
        return 0, 1 # 已删除, 已保留

    return 0, 0 # 不应该执行到这里

def process_directory(perform_actions, source_dir, backup_dir, include_similar, enable_console_log, delete_soft, trash_dir, num_threads, min_size_bytes, simple_backup, simple_backup_with_path):
    seen_hashes = {}
    phash_cache = {}
    phash_list = []
    files = [f for f in source_dir.rglob('*')
             if f.is_file() and
                 os.access(f, os.W_OK) and
                 not f.name.lower().endswith(('.tmp', '.db')) and
                 f.suffix.lower() in IMAGE_EXTENSIONS and
                 f.stat().st_size >= min_size_bytes
             ]
    scanned_count = len(files)
    deleted_count = 0
    retained_count = 0

    print(f"\t🔍 开始扫描目录: {source_dir}, 共找到 {scanned_count} 个文件 (最小大小: {min_size_bytes // 1024} KB), 包括视频: True") # 简化处理，默认包含视频

    if include_similar:
        print("\t🎨 预先计算感知哈希...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(calculate_phash, file, phash_cache) for file in files]
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                progress = (i + 1) / scanned_count * 100
                print(f"\t\r📝 感知哈希计算进度: {progress:.2f}% ({i + 1}/{scanned_count})", end="")
        print("\n\t✅ 感知哈希计算完成.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_file, file, seen_hashes, phash_cache, phash_list, perform_actions, backup_dir, delete_soft, trash_dir, enable_console_log, source_dir, min_size_bytes, simple_backup, simple_backup_with_path) for i, file in enumerate(files)]
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            deleted, retained = future.result()
            deleted_count += deleted
            retained_count += retained
            progress = (i + 1) / scanned_count * 100
            print(f"\t\r📝 处理进度: {progress:.2f}% ({i + 1}/{scanned_count}), 已删除: {deleted_count}, 已保留: {retained_count}", end="")

    print("\n\t✅ 扫描完成")
    return scanned_count, retained_count, deleted_count

# ===== 主程序入口 =====
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="\t清理重复照片和视频，优先保留含GPS信息的文件 (默认模拟执行)")
    parser.add_argument("source_dir", nargs='?', type=str, help="\t要处理的图片和视频目录 (留空则提示输入)")
    parser.add_argument("--optional-source-dir", nargs='?', type=str, help="\t可选的第二个图片和视频目录")
    parser.add_argument("backup_dir", nargs='?', type=str, help="\t被删除文件的备份目录 (留空则提示输入)")
    parser.add_argument("-d", action="store_true", help="\t执行实际的删除和备份操作")
    parser.add_argument("--include-similar", action="store_true", help="\t启用感知哈希比对，删除相似图片")
    parser.add_argument("-log", action="store_true", help="\t将日志同时输出到控制台")
    parser.add_argument("--delete-soft", action="store_true", help="\t启用软删除（移动到指定目录）")
    parser.add_argument("--trash-dir", nargs='?', type=str, help="\t指定软删除目录 (留空则提示输入)")
    parser.add_argument("--log-dir", nargs='?', type=str, help="\t指定日志文件输出目录 (留空则提示输入)")
    parser.add_argument("--hash-threshold", type=int, default=HASH_THRESHOLD, help=f"\t相似图片哈希值阈值 (默认: {HASH_THRESHOLD})")
    parser.add_argument("--threads", type=int, default=4, help="\t设置处理线程数 (默认: 4)") # 添加线程数参数
    parser.add_argument("--prefer-resolution", action="store_true", help="\t对于相似图片，优先保留分辨率更高的版本")
    parser.add_argument("-m", type=int, default=DEFAULT_MIN_SIZE_KB, help=f"\t设置最小扫描文件大小 (KB, 默认: {DEFAULT_MIN_SIZE_KB} KB)") # 添加最小文件大小参数
    parser.add_argument("-v", "--include-videos", action="store_true", help="\t包含视频文件进行检测 (支持 .mp4, .avi, .mov, .mkv)") # 添加包含视频文件参数
    parser.add_argument("-s", "--simple-backup", action="store_true", help="\t启用简单备份模式：仅保留原始文件名，相同文件名时按源文件路径备份") # 添加简单备份模式参数
    parser.add_argument("-s1", action="store_true", help="\t启用备份模式：原始路径+原始文件名+后缀名") # 新增的备份模式参数
    args = parser.parse_args()

    source_directories = []

    if not args.source_dir:
        args.source_dir = input("\n\t请输入要处理的第一个图片和视频目录: ")
    source_directory_1 = Path(args.source_dir)
    source_directories.append(source_directory_1)

    if args.optional_source_dir:
        source_directory_2 = Path(args.optional_source_dir)
        source_directories.append(source_directory_2)

    if not args.backup_dir:
        args.backup_dir = input("\t请输入被删除文件的备份目录: ")
    backup_directory = Path(args.backup_dir)

    perform_actions = args.d
    include_similar = args.include_similar
    enable_console_log = args.log
    delete_soft = args.delete_soft
    num_threads = args.threads
    prefer_resolution = args.prefer_resolution
    min_size_kb = args.m
    min_size_bytes = min_size_kb * 1024
    include_videos = args.include_videos
    simple_backup = args.simple_backup
    simple_backup_with_path = args.s1 # 获取新的备份模式参数

    if include_videos:
        print("\t⚠️\u00A0 视频文件的相似性检测目前仅基于文件哈希，不进行内容分析。")
    if simple_backup:
        print("\tℹ️\u00A0 启用简单备份模式。")
    if simple_backup_with_path:
        print("\tℹ️\u00A0 启用备份模式：原始路径+原始文件名+后缀名。")

    trash_dir = args.trash_dir
    if delete_soft and not trash_dir:
        trash_dir = input("\t请输入软删除目录: ")
    trash_directory = Path(trash_dir) if trash_dir else None

    log_dir = args.log_dir
    if log_dir:
        log_file_path = Path(log_dir) / DEFAULT_LOG_FILE
    else:
        log_file_path = backup_directory / DEFAULT_LOG_FILE
        if not perform_actions: # 如果是模拟运行，提示用户
            print(f"\t[模拟] 日志文件将保存到备份目录下: {log_file_path}")
        else:
            print(f"\t日志文件将保存到备份目录下: {log_file_path}")

    logging.basicConfig(filename=str(log_file_path), level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    print(f"\t运行模式: {'执行' if perform_actions else '模拟'}")
    print(f"\t包含相似图片: {include_similar}")
    print(f"\t相似度哈希阈值: {args.hash_threshold}")
    print(f"\t使用线程数: {num_threads}")
    print(f"\t优先保留高分辨率: {prefer_resolution}")
    print(f"\t最小扫描文件大小: {min_size_kb} KB")
    print(f"\t同时输出日志到控制台: {enable_console_log}")
    print(f"\t包含视频文件: {include_videos}")
    print(f"\t简单备份模式(-s): {simple_backup}")
    print(f"\t原始路径备份模式(-s1): {simple_backup_with_path}")
    print(f"\t软删除: {delete_soft}")
    if delete_soft:
        print(f"\t回收站目录: {trash_directory}")

    all_scanned_count = 0
    all_retained_count = 0
    all_deleted_count = 0

    for source_dir in source_directories:
        if source_dir.exists() and backup_directory.exists():
            scanned_count, retained_count, deleted_count = process_directory(
                perform_actions=perform_actions,
                source_dir=source_dir,
                backup_dir=backup_directory,
                include_similar=include_similar,
                enable_console_log=enable_console_log,
                delete_soft=delete_soft,
                trash_dir=trash_directory,
                num_threads=num_threads,
                min_size_bytes=min_size_bytes,
                simple_backup=simple_backup,
                simple_backup_with_path=simple_backup_with_path
            )
            all_scanned_count += scanned_count
            all_retained_count += retained_count
            all_deleted_count += deleted_count
        else:
            logging.error(f"\t❌ 目录不存在: {source_dir} 或 {backup_directory}")
            print(f"\t❌ 错误: 请检查目录是否存在 - {source_dir} 或 {backup_directory}")

    print(f"\t✅ 扫描完成，共扫描 {all_scanned_count} 个文件，保留 {all_retained_count} 个文件，删除 {all_deleted_count} 个文件\n")
