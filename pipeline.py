import subprocess
import re
import time
import json
import shutil
import logging
import sys
import os
import csv
import concurrent.futures
import threading
import multiprocessing
from pathlib import Path

# ================== CONFIG ==================

# Everything is anchored to the folder this script lives in, so it works the same
# way regardless of where you launch it from -- and works out of the box on a fork.
BASE_DIR = Path(__file__).resolve().parent

EXTRACTOR_SCRIPT = BASE_DIR / "reddit_zst_filter_zstandard.py"

OUTPUT_DIR = BASE_DIR / "output"                # extractor writes one CSV per input file here
FINAL_OUTPUT = BASE_DIR / "final_dataset.csv"   # merged dataset

STATE_FILE = BASE_DIR / "pipeline_state.json"
LOG_FILE = BASE_DIR / "pipeline.log"

SUBREDDITS = (
    "chatgpt,openai,claudeai,anthropic,artificial,singularity,machinelearning,"
    "technology,futurology,"
    "cscareerquestions,webdev,itcareerquestions,learnprogramming,datascience,"
    "programming,experienceddevs,softwareengineering,"
    "jobs,careerguidance,recruitinghell,resumes,layoffs,antiwork,"
    "copywriting,graphic_design,freelance,translation,journalism,artistlounge"
)

SUBREDDIT_SET = {s.strip().lower() for s in SUBREDDITS.split(",") if s.strip()}

POLL_INTERVAL_SEC = 120        # how often to re-check folders for new/finished files
MIN_FREE_GB = 15               # pause and warn if free disk space drops below this
DELETE_SOURCE_AFTER_EXTRACT = False  # keep .zst after extraction

# NOTE: everything below this point used to live here, at module level --
# that was the bug. On Windows, multiprocessing's default "spawn" start method
# re-imports this whole module in every worker process. Anything with a side
# effect (print, input(), directory scans, logging.basicConfig) that sits
# outside `if __name__ == "__main__":` runs AGAIN in each worker, before the
# worker ever gets a chance to execute the actual task it was spawned for.
# Here that meant every HDD worker process re-ran the interactive config
# wizard and got stuck on input() with no attached stdin -- so the pool
# never really started, and all 14 cores sat idle while the single producer
# thread did the only real work alone (hence 9% CPU / Disk 1 at 0%).
# Interactive config, directory scanning, logging setup, and the call to
# main() now all live inside the __main__ guard below, so spawned workers
# skip straight past this block.

def get_input(prompt, validator, error_msg):
    """Repeatedly ask for input until a valid value is provided."""
    while True:
        val = input(prompt).strip()
        if validator(val):
            return val
        print(error_msg)


def _parse_file_selection(raw: str, total: int):
    """Parse user input like 'all', '1,3,5', '1-4,7', '2-5' into a sorted
    list of 0-based indices.  Returns None on invalid input."""
    raw = raw.strip().lower()
    if raw == "all":
        return list(range(total))
    indices = set()
    for part in raw.split(","):
        part = part.strip()
        if "-" in part:
            bounds = part.split("-", 1)
            if len(bounds) != 2 or not bounds[0].isdigit() or not bounds[1].isdigit():
                return None
            lo, hi = int(bounds[0]), int(bounds[1])
            if lo < 1 or hi > total or lo > hi:
                return None
            indices.update(range(lo - 1, hi))
        elif part.isdigit():
            idx = int(part)
            if idx < 1 or idx > total:
                return None
            indices.add(idx - 1)
        else:
            return None
    return sorted(indices) if indices else None


# ================== STATE (so the script can be restarted without losing progress) ==================

state_lock = threading.Lock()

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"processed": []}

def save_state(state):
    with state_lock:
        STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

# ================== FILE / DISK HELPERS ==================

def is_fully_downloaded(path: Path):
    """File exists, isn't a torrent-client temp file, and its size is stable (fully downloaded)."""
    if not path.exists():
        return False
    if path.with_suffix(path.suffix + ".!qB").exists():
        return False
    size1 = path.stat().st_size
    time.sleep(5)
    size2 = path.stat().st_size
    return size1 == size2 and size1 > 0

def free_space_gb(path: Path = BASE_DIR):
    return shutil.disk_usage(path).free / (1024 ** 3)

def output_csv_path(filename):
    stem = Path(filename).stem  # e.g. RC_2023-11
    return OUTPUT_DIR / f"{stem}.csv"

# ================== SSD MODE: PER-FILE PROCESSING VIA SUBPROCESS ==================

def process_file_ssd(filepath: Path, state):
    """Extract locally from a raw .zst by invoking the extractor subprocess.
    Good for SSD: multiple files can be processed concurrently without
    head-seek penalties."""
    filename = filepath.name
    folder = filepath.parent

    if not is_fully_downloaded(filepath):
        log.info(f"{filename}: still downloading or not found yet, waiting...")
        return False

    out_path = output_csv_path(filename)
    if out_path.exists():
        log.info(f"{filename}: output already exists, skipping re-extraction")
        state["processed"].append(filename)
        save_state(state)
        return True

    free_gb = free_space_gb()
    if free_gb < MIN_FREE_GB:
        log.warning(f"Low disk space: {free_gb:.1f}GB free (threshold {MIN_FREE_GB}GB). Waiting...")
        return False

    log.info(f"Starting extraction of {filename} (free space {free_gb:.1f}GB)...")

    # file_filter is a regex over filenames INSIDE the target folder, not a path,
    # so we point the extractor at the correct subfolder and narrow it to this one file
    escaped_name = re.escape(filename)
    command = [
        sys.executable, str(EXTRACTOR_SCRIPT),
        str(folder),
        "--output_dir", str(OUTPUT_DIR),
        "--format", "csv",
        "--field", "subreddit",
        "--value", SUBREDDITS,
        "--chunk_size", CHUNK_SIZE_STR,
        "--file_filter", f"^{escaped_name}$",
        "--config", str(BASE_DIR / "config.json"),
    ]

    try:
        # cwd is pinned explicitly: the extractor's own --config default is a
        # relative path ("config.json"), so if the subprocess starts in the
        # wrong working directory it crashes at startup before doing anything.
        # creationflags=subprocess.CREATE_NEW_CONSOLE opens a separate terminal
        # window for each file so you can see the tqdm progress bar in SSD mode.
        result = subprocess.run(
            command,
            cwd=str(BASE_DIR),
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            timeout=6 * 3600,
        )
    except subprocess.TimeoutExpired:
        log.error(f"{filename}: timed out after 6h, will retry next cycle")
        return False

    if result.returncode != 0:
        log.error(f"{filename}: extraction failed (code {result.returncode}). See terminal window for errors.")
        return False

    log.info(f"Successfully processed: {filename}")
    state["processed"].append(filename)
    save_state(state)

    if DELETE_SOURCE_AFTER_EXTRACT:
        try:
            filepath.unlink()
            log.info(f"Deleted source file {filename}, disk space reclaimed")
        except OSError as e:
            log.warning(f"Could not delete {filename}: {e}")

    return True

# ================== HDD MODE: PRODUCER-CONSUMER WITH MULTIPROCESSING ==================
#
# Architecture:
#   Producer (single thread)  — reads .zst sequentially, decompresses, splits on \n
#                                boundaries, feeds byte chunks into a work queue.
#   Worker pool (N processes) — each worker receives a chunk of complete JSON lines,
#                                filters for target subreddits, returns matched records.
#   Consumer (main thread)    — collects filtered results and writes them to the CSV.
#
# Why multiprocessing instead of threading:
#   JSON parsing + regex matching is CPU-bound Python. The GIL would serialize all
#   that work onto a single core. Separate processes each get their own GIL.
#
# Chunk boundary handling:
#   The reader never cuts in the middle of a line. It reads raw decompressed bytes,
#   finds the last b'\n', sends everything up to (and including) it, and carries
#   the tail over to the next iteration.
#
# IPC efficiency:
#   Chunks are kept at ~16 MB to balance throughput vs. pickle serialization overhead.

_SENTINEL = None  # signals workers that no more data is coming


def _hdd_worker_loop(work_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue, target_subreddits: set):
    """Worker process loop. Pulls chunks from work_queue, filters, puts results in result_queue."""
    import orjson
    needles = [sub.encode("utf-8") for sub in target_subreddits]

    while True:
        chunk_bytes = work_queue.get()
        if chunk_bytes is _SENTINEL:
            result_queue.put(_SENTINEL)
            break

        matched = []
        lines_in_chunk = 0

        for raw_line in chunk_bytes.split(b"\n"):
            if not raw_line:
                continue
            lines_in_chunk += 1

            raw_lower = raw_line.lower()
            if not any(needle in raw_lower for needle in needles):
                continue

            try:
                obj = orjson.loads(raw_line)
                sub = obj.get("subreddit", "")
                if isinstance(sub, str) and sub.lower() in target_subreddits:
                    matched.append(obj)
            except Exception:
                pass

        result_queue.put((matched, lines_in_chunk))


def _hdd_producer(filepath: Path, work_queue: multiprocessing.Queue,
                  chunk_bytes_size: int, num_workers: int):
    """Single-threaded reader that streams decompressed data from a .zst file.

    Reads the compressed file in ~4-8 MB I/O blocks (handled internally by
    zstandard), decompresses into a streaming byte buffer, slices at newline
    boundaries into ~16 MB chunks of complete lines, and pushes them into
    the shared work_queue.

    After the file is fully read, pushes one _SENTINEL per worker so every
    worker knows to shut down.
    """
    import zstandard
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("pipeline")

    compressed_size = filepath.stat().st_size
    log.info(
        f"[HDD Producer] Starting sequential read of {filepath.name} "
        f"({compressed_size / (1024**3):.2f} GB compressed)"
    )
    total_bytes_decompressed = 0
    chunks_sent = 0
    tail = b""  # leftover bytes after the last \n in the previous read

    try:
        with open(filepath, "rb") as fh:
            dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
            with dctx.stream_reader(fh) as reader:
                while True:
                    raw = reader.read(chunk_bytes_size)
                    if not raw:
                        break
                    total_bytes_decompressed += len(raw)

                    data = tail + raw

                    # Find the last newline — everything before it is safe to
                    # hand off (complete lines only). Everything after it is
                    # an incomplete line that must be glued to the next read.
                    last_nl = data.rfind(b"\n")
                    if last_nl == -1:
                        # No newline at all — the entire buffer is a partial
                        # line (extremely rare for 16 MB reads, but possible
                        # for a single gigantic JSON object).
                        tail = data
                        continue

                    payload = data[: last_nl + 1]  # include the trailing \n
                    tail = data[last_nl + 1 :]

                    work_queue.put(payload)
                    chunks_sent += 1

                    if chunks_sent % 50 == 0:
                        # Use the underlying file handle position for % progress.
                        # fh.tell() gives compressed bytes read so far.
                        compressed_pos = fh.tell()
                        pct = (compressed_pos / compressed_size * 100) if compressed_size else 0
                        log.info(
                            f"[HDD Producer] {filepath.name}: "
                            f"{total_bytes_decompressed / (1024**3):.2f} GB decompressed, "
                            f"{chunks_sent} chunks queued, "
                            f"{pct:.1f}% of compressed file read"
                        )

        # Flush any remaining tail (last line without a trailing \n).
        if tail:
            work_queue.put(tail)
            chunks_sent += 1

    except Exception as e:
        log.error(f"[HDD Producer] Error reading {filepath.name}: {e}")
    finally:
        # Signal every worker to stop.
        for _ in range(num_workers):
            work_queue.put(_SENTINEL)
        log.info(
            f"[HDD Producer] Finished {filepath.name}: "
            f"{total_bytes_decompressed / (1024**3):.2f} GB decompressed, "
            f"{chunks_sent} chunks total"
        )


def process_file_hdd(filepath: Path, state):
    """Process a single .zst file using the HDD-optimized producer-consumer pipeline.

    1. A single producer thread reads and decompresses the file sequentially.
    2. A pool of worker *processes* (not threads) filters each chunk in parallel.
    3. The main thread (consumer) collects matched records and writes the CSV.
    """
    filename = filepath.name

    if not is_fully_downloaded(filepath):
        log.info(f"{filename}: still downloading or not found yet, waiting...")
        return False

    out_path = output_csv_path(filename)
    if out_path.exists():
        log.info(f"{filename}: output already exists, skipping re-extraction")
        state["processed"].append(filename)
        save_state(state)
        return True

    free_gb = free_space_gb()
    if free_gb < MIN_FREE_GB:
        log.warning(f"Low disk space: {free_gb:.1f}GB free (threshold {MIN_FREE_GB}GB). Waiting...")
        return False

    log.info(f"[HDD] Starting producer-consumer extraction of {filename} (free space {free_gb:.1f}GB)...")

    num_workers = HDD_NUM_WORKERS
    # maxsize limits memory pressure, but also sets how far ahead the reader
    # can get before blocking. 2x was too shallow -- the disk was idling most
    # of the time waiting for the queue to drain (observed: 15% active time,
    # 28 MB/s on a drive capable of 80-150 MB/s sequential). 8x lets the
    # producer read several chunks ahead per worker, smoothing disk I/O into
    # something closer to continuous. Each chunk is ~16-32MB, so at 8x with
    # a dozen workers this is a few hundred MB of RAM at most -- cheap given
    # the 32GB available.
    # maxsize limits memory pressure. 64MB chunks * num_workers * 2 = ~1.5GB RAM
    work_queue = multiprocessing.Queue(maxsize=num_workers * 2)
    result_queue = multiprocessing.Queue()

    # --- Start the producer in a separate process to avoid GIL contention with main process queue getting ---
    producer_process = multiprocessing.Process(
        target=_hdd_producer,
        args=(filepath, work_queue, HDD_READER_CHUNK_BYTES, num_workers),
        daemon=True,
    )
    producer_process.start()

    # --- Start worker processes ---
    workers = []
    for _ in range(num_workers):
        w = multiprocessing.Process(
            target=_hdd_worker_loop,
            args=(work_queue, result_queue, SUBREDDIT_SET),
            daemon=True,
        )
        w.start()
        workers.append(w)

    # --- Consumer (main thread) ---
    all_matched = []
    total_chunks_processed = 0
    total_lines_scanned = 0
    total_matched_count = 0
    last_logged_lines = 0
    PROGRESS_LOG_INTERVAL = 500_000
    workers_done = 0

    while workers_done < num_workers:
        res = result_queue.get()
        if res is _SENTINEL:
            workers_done += 1
            continue

        matched, lines_count = res
        total_chunks_processed += 1
        total_lines_scanned += lines_count
        if matched:
            total_matched_count += len(matched)
            all_matched.extend(matched)

        while total_lines_scanned - last_logged_lines >= PROGRESS_LOG_INTERVAL:
            last_logged_lines += PROGRESS_LOG_INTERVAL
            log.info(
                f"[HDD] {filename}: {total_lines_scanned:,} lines scanned, "
                f"{total_matched_count:,} matched"
            )

    producer_process.join()
    for w in workers:
        w.join()

    log.info(
        f"[HDD] {filename}: filtering done. "
        f"{total_chunks_processed} chunks processed, "
        f"{total_lines_scanned:,} lines scanned, "
        f"{total_matched_count:,} records matched."
    )

    # --- Write results to CSV ---
    if all_matched:
        try:
            import pandas as pd
            from reddit_filter_utils import Config, DataNormalizer

            config = Config(str(BASE_DIR / "config.json"))
            df = pd.DataFrame(all_matched)
            df = DataNormalizer.normalize_dataframe(df, config)
            df.to_csv(str(out_path), index=False)
            log.info(f"[HDD] Wrote {len(df)} records to {out_path}")
        except Exception as e:
            log.error(f"[HDD] Failed to write CSV for {filename}: {e}")
            return False
    else:
        log.info(f"[HDD] {filename}: 0 records matched, no output file created.")

    state["processed"].append(filename)
    save_state(state)

    if DELETE_SOURCE_AFTER_EXTRACT:
        try:
            filepath.unlink()
            log.info(f"Deleted source file {filename}, disk space reclaimed")
        except OSError as e:
            log.warning(f"Could not delete {filename}: {e}")

    return True

# ================== FINAL MERGE ==================

def merge_final_dataset():
    import pandas as pd

    csv_files = [output_csv_path(f.name) for f in TARGET_FILES]
    existing = [f for f in csv_files if f.exists()]

    if len(existing) != len(csv_files):
        missing = set(csv_files) - set(existing)
        log.warning(f"Missing {len(missing)} CSV files for the final merge: {missing}")
        # Comment out the return below if you want to merge currently available files 
        # without waiting for the entire dataset to finish downloading/extracting.
        return

    log.info(f"Merging final dataset from {len(existing)} files...")

    AI_NATIVE_SUBREDDITS = {
        "chatgpt", "openai", "claude", "anthropic",
        "artificial", "singularity", "machinelearning",
    }

    keyword_pattern = (
        r'\bai\b|\bartificial intelligence\b|\bchatgpt\b|\bopenai\b|\bclaude\b|\banthropic\b|'
        r'\bgpt\w*\b|\bllm\w*\b|\bmachine learning\b|\bcopilot\b|\bmidjourney\b|\bdall-?e\b|'
        r'\bbard\b|\bgemini\b|\bgenerative ai\b|\bgenai\w*\b|'
        r'\breplac\w*\b|\bobsolete\b|\blayoff\w*\b|\bautomat\w*\b|\buseless\b|'
        r'\btakeover\w*\b|\bredundant\w*\b'
    )

    columns_to_keep = ['id', 'created_utc', 'subreddit', 'author', 'score', 'body', 'title', 'selftext']

    dfs = []
    for f in existing:
        try:
            log.info(f"Processing and filtering {f.name}...")
            df = pd.read_csv(f, low_memory=False)
            cols = [c for c in columns_to_keep if c in df.columns]
            df = df[cols]
            text_columns = [c for c in ['body', 'title', 'selftext'] if c in df.columns]
            for col in text_columns:
                df[col] = df[col].fillna('')

            is_ai_native = (
                df['subreddit'].str.lower().isin(AI_NATIVE_SUBREDDITS)
                if 'subreddit' in df.columns else pd.Series(False, index=df.index)
            )
            keyword_mask = pd.Series(False, index=df.index)
            for col in text_columns:
                keyword_mask = keyword_mask | df[col].str.contains(keyword_pattern, case=False, regex=True)
            mask = is_ai_native | keyword_mask
            filtered_df = df[mask]

            log.info(f"Kept {len(filtered_df)} rows out of {len(df)} in {f.name}")
            dfs.append(filtered_df)
        except Exception as e:
            log.error(f"Could not read/process {f}: {e}")

    if not dfs:
        log.error("No CSV could be read or matched keywords, final file not created.")
        return

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv(FINAL_OUTPUT, index=False)
    log.info(f"Done! {FINAL_OUTPUT} -- {len(final_df)} rows, {len(final_df.columns)} columns")

# ================== MAIN LOOP ==================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()

    log.info(f"Pipeline started in {DISK_TYPE.upper()} mode. Base dir: {BASE_DIR}")
    log.info(f"Found {len(TARGET_FILES)} .zst files to process in {DATA_SOURCE_DIR}:")
    for f in TARGET_FILES:
        log.info(f"  - {f.name}")

    if DISK_TYPE == "ssd":
        _main_ssd(state)
    else:
        _main_hdd(state)


def _main_ssd(state):
    active_futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FILES) as executor:
        while True:
            finished_tasks = [fname for fname, fut in active_futures.items() if fut.done()]
            for fname in finished_tasks:
                del active_futures[fname]

            all_done = True
            for filepath in TARGET_FILES:
                filename = filepath.name
                if filename not in state["processed"]:
                    all_done = False
                    if output_csv_path(filename).exists():
                        state["processed"].append(filename)
                        save_state(state)
                        continue
                    if is_fully_downloaded(filepath) and free_space_gb() >= MIN_FREE_GB:
                        if filename not in active_futures:
                            active_futures[filename] = executor.submit(process_file_ssd, filepath, state)

            if all_done and not active_futures:
                log.info("All files accounted for.")
                if MERGE_AT_END:
                    merge_final_dataset()
                else:
                    log.info("Skipping final merge as requested.")
                break

            time.sleep(60)


def _main_hdd(state):
    for filepath in TARGET_FILES:
        filename = filepath.name
        if filename in state["processed"]:
            log.info(f"{filename}: already processed, skipping")
            continue

        if output_csv_path(filename).exists():
            log.info(f"{filename}: output CSV already exists, marking as done")
            state["processed"].append(filename)
            save_state(state)
            continue

        while not is_fully_downloaded(filepath):
            log.info(f"{filename}: waiting for download to complete...")
            time.sleep(60)

        while free_space_gb() < MIN_FREE_GB:
            log.warning(f"Low disk space ({free_space_gb():.1f}GB). Waiting...")
            time.sleep(60)

        process_file_hdd(filepath, state)

    log.info("All files accounted for.")
    if MERGE_AT_END:
        merge_final_dataset()
    else:
        log.info("Skipping final merge as requested.")


# ================== EVERYTHING BELOW ONLY RUNS IN THE REAL MAIN PROCESS ==================
# This is the fix: interactive config, directory scanning, logging setup, and the
# call to main() all live here now. Spawned worker processes re-import this file
# (spawn start method on Windows) but their __name__ is the module name, not
# "__main__" -- so they skip straight past this block and never touch input().

if __name__ == "__main__":
    print("=== Pipeline Configuration ===")

    DISK_TYPE = get_input(
        "Disk type where .zst files are stored (ssd/hdd): ",
        lambda x: x.lower() in ["ssd", "hdd"],
        "Invalid input. Please enter 'ssd' or 'hdd'."
    ).lower()

    CHUNK_SIZE_STR = "0"       # placeholder, only meaningful in SSD mode
    MAX_CONCURRENT_FILES = 1   # placeholder, only meaningful in SSD mode

    if DISK_TYPE == "ssd":
        CHUNK_SIZE_STR = get_input(
            "Enter chunk size in lines (e.g. 250000): ",
            lambda x: x.isdigit() and int(x) > 0,
            "Invalid input. Please enter a positive integer (digits only)."
        )
        MAX_CONCURRENT_FILES = int(get_input(
            "Enter max concurrent files (e.g. 3): ",
            lambda x: x.isdigit() and int(x) > 0,
            "Invalid input. Please enter a positive integer."
        ))

    MERGE_AT_END = get_input(
        "Merge at the end? (yes/no): ",
        lambda x: x.lower() in ['yes', 'y', 'no', 'n'],
        "Invalid input. Please enter 'yes' or 'no'."
    ).lower() in ['yes', 'y']

    data_source_str = get_input(
        "Enter data (input) source directory (where .zst files are located): ",
        lambda x: Path(x).is_dir(),
        "Invalid input. Please enter a valid existing directory path."
    )

    DATA_SOURCE_DIR = Path(data_source_str).resolve()
    _all_zst = sorted(DATA_SOURCE_DIR.rglob("*.zst"))
    if not _all_zst:
        raise SystemExit(f"No .zst files found in {DATA_SOURCE_DIR}")

    print(f"\nFound {len(_all_zst)} .zst files:")
    for i, f in enumerate(_all_zst, 1):
        rel = f.relative_to(DATA_SOURCE_DIR)
        size_gb = f.stat().st_size / (1024 ** 3)
        print(f"  [{i:>2}] {rel}  ({size_gb:.2f} GB)")

    _selection_str = get_input(
        "\nSelect files to process (e.g. 'all', '1,3,5', '1-4,7'): ",
        lambda x: _parse_file_selection(x, len(_all_zst)) is not None,
        f"Invalid input. Enter 'all' or numbers/ranges between 1 and {len(_all_zst)}."
    )
    _selected_indices = _parse_file_selection(_selection_str, len(_all_zst))
    TARGET_FILES = [_all_zst[i] for i in _selected_indices]
    print(f"Selected {len(TARGET_FILES)} file(s) for processing.")

    if DISK_TYPE == "hdd":
        default_workers = max(1, os.cpu_count() - 2)
        HDD_NUM_WORKERS = int(get_input(
            f"Enter number of filter worker processes (default {default_workers}): ",
            lambda x: x.isdigit() and int(x) > 0,
            "Invalid input. Please enter a positive integer."
        ))
        HDD_READER_CHUNK_BYTES = 64 * 1024 * 1024

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
    )
    log = logging.getLogger("pipeline")

    main()