import subprocess
import re
import time
import json
import shutil
import logging
import sys
import concurrent.futures
import threading
from pathlib import Path

# ================== CONFIG ==================

# Everything is anchored to the folder this script lives in, so it works the same
# way regardless of where you launch it from -- and works out of the box on a fork.
BASE_DIR = Path(__file__).resolve().parent

EXTRACTOR_SCRIPT = BASE_DIR / "reddit_zst_filter_zstandard.py"

COMMENTS_DIR = BASE_DIR / "data" / "reddit" / "comments"       # RC_*.zst live here
SUBMISSIONS_DIR = BASE_DIR / "data" / "reddit" / "submissions" # RS_*.zst live here

OUTPUT_DIR = BASE_DIR / "output"                # extractor writes one CSV per input file here
FINAL_OUTPUT = BASE_DIR / "final_dataset.csv"   # merged dataset

STATE_FILE = BASE_DIR / "pipeline_state.json"
LOG_FILE = BASE_DIR / "pipeline.log"

SUBREDDITS = (
    "cscareerquestions,jobs,recruitinghell,copywriting,graphic_design,"
    "ArtistLounge,ChatGPT,artificial,singularity,Futurology"
)

# Who is running this copy of the script. This lives in a local, git-ignored file
# (not in this source file) so that two collaborators pushing to the same repo
# never overwrite each other's role. To set your role, create role.local.txt next
# to this script containing just "me" or "partner" (no quotes, no newline needed).
ROLE_FILE = BASE_DIR / "role.local.txt"
if ROLE_FILE.exists():
    WHO_AM_I = ROLE_FILE.read_text(encoding="utf-8").strip()
else:
    raise SystemExit(
        f"Missing {ROLE_FILE}. Create it and put 'me' or 'partner' inside "
        f"(whichever role this machine handles) before running the pipeline."
    )

# Individual hardware configuration
if WHO_AM_I == "me":
    # Your powerful laptop (32GB RAM, 14 cores)
    MAX_CONCURRENT_FILES = 6
    CHUNK_SIZE_STR = "500000"
else:
    # Partner's laptop (16GB RAM, unknown CPU)
    # 250,000 requires ~2GB per thread, 3 threads will take a safe 6GB of RAM
    MAX_CONCURRENT_FILES = 3
    CHUNK_SIZE_STR = "250000"

MONTHS_BY_PERSON = {
    "me": ["2023-11", "2024-04", "2024-11"],
    "partner": ["2022-04", "2022-12", "2023-04"],
}

OWN_MONTHS = MONTHS_BY_PERSON[WHO_AM_I]
OTHER_MONTHS = [m for person, months in MONTHS_BY_PERSON.items() if person != WHO_AM_I for m in months]
ALL_MONTHS = OWN_MONTHS + OTHER_MONTHS
TARGET_FILES = [f"{prefix}_{m}.zst" for m in ALL_MONTHS for prefix in ("RS", "RC")]

POLL_INTERVAL_SEC = 120        # how often to re-check folders for new/finished files
MIN_FREE_GB = 15               # pause and warn if free disk space drops below this
DELETE_SOURCE_AFTER_EXTRACT = True   # delete .zst right after successful extraction (disk space is tight)

# ================== LOGGING ==================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("pipeline")

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

def source_dir(filename):
    """RC_ files live in comments/, RS_ files live in submissions/."""
    return COMMENTS_DIR if filename.startswith("RC_") else SUBMISSIONS_DIR

def month_of(filename):
    """RC_2023-11.zst -> '2023-11'"""
    return Path(filename).stem.split("_", 1)[1]

def is_own_file(filename):
    return month_of(filename) in OWN_MONTHS

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

# ================== PER-FILE PROCESSING ==================

def process_own_file(filename, state):
    """Extract locally from a raw .zst I downloaded myself."""
    folder = source_dir(filename)
    src_path = folder / filename

    if not is_fully_downloaded(src_path):
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
    ]

    try:
        result = subprocess.run(command, timeout=6 * 3600, creationflags=subprocess.CREATE_NEW_CONSOLE)
    except subprocess.TimeoutExpired:
        log.error(f"{filename}: timed out after 6h, will retry next cycle")
        return False

    if result.returncode != 0:
        log.error(f"{filename}: extraction failed with code {result.returncode}. Check its separate console window for details.")
        return False

    log.info(f"Successfully processed: {filename}")
    state["processed"].append(filename)
    save_state(state)

    if DELETE_SOURCE_AFTER_EXTRACT:
        try:
            src_path.unlink()
            log.info(f"Deleted source file {filename}, disk space reclaimed")
        except OSError as e:
            log.warning(f"Could not delete {filename}: {e}")

    return True

def process_teammate_file(filename, state):
    """No local .zst for these -- just wait until the teammate drops the finished CSV in OUTPUT_DIR."""
    out_path = output_csv_path(filename)
    if out_path.exists():
        log.info(f"{filename}: CSV from teammate found in {OUTPUT_DIR}")
        state["processed"].append(filename)
        save_state(state)
        return True

    log.info(f"{filename}: waiting for teammate to drop the CSV into {OUTPUT_DIR}/")
    return False

def process_file(filename, state):
    if filename in state["processed"]:
        return True
    if is_own_file(filename):
        return process_own_file(filename, state)
    else:
        return process_teammate_file(filename, state)

# ================== FINAL MERGE ==================

def merge_final_dataset():
    import pandas as pd

    csv_files = [output_csv_path(fn) for fn in TARGET_FILES]
    existing = [f for f in csv_files if f.exists()]

    if len(existing) != len(csv_files):
        missing = set(csv_files) - set(existing)
        log.warning(f"Missing {len(missing)} CSV files for the final merge: {missing}")
        # Comment out the return below if you want to merge currently available files 
        # without waiting for the entire dataset to finish downloading/extracting.
        return

    log.info(f"Merging final dataset from {len(existing)} files...")
    
    # Keywords for lexical filtering (AI Anxiety focus)
    keywords = "ai|artificial intelligence|chatgpt|openai|claude|gpt|replaced|obsolete|layoff|automate|useless|takeover"
    
    # Core columns needed for analysis (discarding irrelevant metadata)
    columns_to_keep = ['id', 'created_utc', 'subreddit', 'author', 'score', 'body', 'title', 'selftext']

    dfs = []
    for f in existing:
        try:
            log.info(f"Processing and filtering {f.name}...")
            
            # Read the extracted CSV file
            df = pd.read_csv(f, low_memory=False)
            
            # Keep only the required columns (ignoring those missing in the current file)
            cols = [c for c in columns_to_keep if c in df.columns]
            df = df[cols]
            
            # Determine which text columns are present 
            # ('body' for comments; 'title' and 'selftext' for submissions)
            text_columns = [c for c in ['body', 'title', 'selftext'] if c in df.columns]
            
            # Fill NaN values to prevent .str.contains() from throwing errors
            for col in text_columns:
                df[col] = df[col].fillna('')
                
            # Create a boolean mask: look for keywords in at least one of the text columns
            mask = pd.Series(False, index=df.index)
            for col in text_columns:
                mask = mask | df[col].str.contains(keywords, case=False, regex=True)
                
            # Apply the mask to filter the dataframe
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
    COMMENTS_DIR.mkdir(parents=True, exist_ok=True)
    SUBMISSIONS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()

    log.info(f"Pipeline started as '{WHO_AM_I}'. Base dir: {BASE_DIR}")
    log.info("Waiting for 12 files (own months extracted locally, teammate's months dropped in as CSVs):")
    for f in TARGET_FILES:
        kind = "own (raw .zst)" if is_own_file(f) else "teammate's (finished CSV)"
        log.info(f"  - {f}  [{kind}]")

    active_futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FILES) as executor:
        while True:
            finished_tasks = [fname for fname, fut in active_futures.items() if fut.done()]
            for fname in finished_tasks:
                del active_futures[fname]

            all_done = True
            for filename in TARGET_FILES:
                if filename not in state["processed"]:
                    all_done = False
                    
                    if is_own_file(filename):
                        src_path = source_dir(filename) / filename
                        if is_fully_downloaded(src_path) and free_space_gb() >= MIN_FREE_GB:
                            if filename not in active_futures:
                                active_futures[filename] = executor.submit(process_own_file, filename, state)
                    else:
                        process_teammate_file(filename, state)

            if all_done and not active_futures:
                log.info("All 12 files accounted for.")
                merge_final_dataset()
                break

            time.sleep(60)

if __name__ == "__main__":
    main()