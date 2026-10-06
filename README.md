# Reddit Dump Extractor — AI Anxiety on the Job Market

Data extraction pipeline for **CSS Homework #2 — Dataset Collection and Understanding** (Computational Social Science, National University of Kyiv-Mohyla Academy, 2026).

The project turns raw Pushshift Reddit dumps (`.zst`) into a clean, analysis-ready CSV of Reddit posts and comments about **AI-induced anxiety, job automation and the changing labour market**.

> Built on top of [SanGreel/reddit-dump-extractor](https://github.com/SanGreel/reddit-dump-extractor), which in turn is based on [Watchful1/PushshiftDumps](https://github.com/Watchful1/PushshiftDumps/tree/master/scripts). We added an orchestration layer (`pipeline.py`) on top of the original extractors.

---

## Table of contents

1. [Research context](#research-context)
2. [Dataset at a glance](#dataset-at-a-glance)
3. [How the pipeline works](#how-the-pipeline-works)
4. [Repository structure](#repository-structure)
5. [Installation](#installation)
6. [Quick start (full pipeline)](#quick-start-full-pipeline)
7. [Using the extractors on their own](#using-the-extractors-on-their-own)
8. [Output format](#output-format)
9. [Troubleshooting](#troubleshooting)
10. [Links](#links)
11. [Authors and credits](#authors-and-credits)

---

## Research context

Generative AI went mainstream with the release of ChatGPT. This project asks how public discourse about jobs and automation changed **before and after** that moment, and **who** is most worried: technical workers, creatives, or the general public.

To make that comparison possible, the data is collected from two cohorts of subreddits over two time windows.

**Subreddit cohorts (10 communities)**

| Cohort | Subreddits |
| --- | --- |
| AI / Tech-focused | `ChatGPT`, `Futurology`, `singularity`, `artificial` |
| Career / General | `jobs`, `cscareerquestions`, `recruitinghell`, `ArtistLounge`, `graphic_design`, `copywriting` |

**Time windows (6 monthly dumps, each with comments `RC_` and submissions `RS_`)**

| Phase | Months | Meaning |
| --- | --- | --- |
| Phase 1 | 2022-04, 2022-12, 2023-04 | Stable market and the initial shock of ChatGPT's release |
| Phase 2 | 2023-11, 2024-04, 2024-11 | Peak of corporate AI integration and workforce adaptation |

---

## Dataset at a glance

| Metric | Value |
| --- | --- |
| Source | Pushshift Reddit dumps via [Academic Torrents](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1) |
| Content | Reddit submissions and comments |
| Total size | 473.90 MB |
| Total rows | 951,508 |

Rows per subreddit:

| Subreddit | Rows |
| --- | ---: |
| ChatGPT | 253,883 |
| Futurology | 165,260 |
| singularity | 154,969 |
| jobs | 110,882 |
| cscareerquestions | 94,138 |
| recruitinghell | 77,723 |
| ArtistLounge | 35,917 |
| graphic_design | 34,425 |
| artificial | 18,102 |
| copywriting | 6,209 |
| **Total** | **951,508** |

---

## How the pipeline works

```text
 Pushshift .zst dumps (RC_* / RS_*)
            │
            ▼
 ┌──────────────────────────────┐
 │ 1. Extraction (per file)     │  reddit_zst_filter_zstandard.py
 │    keep only the 10 target   │  streamed in chunks, one CSV per input file
 │    subreddits                │
 └──────────────┬───────────────┘
                ▼
 ┌──────────────────────────────┐
 │ 2. Merge + keyword filter    │  pipeline.py → merge_final_dataset()
 │    regex over body / title / │  keeps only the columns needed for analysis
 │    selftext                  │
 └──────────────┬───────────────┘
                ▼
 ┌──────────────────────────────┐
 │ 3. Cleaning                  │  clean_heuristic.py
 │    bots, deleted/removed,    │
 │    exact duplicates          │
 └──────────────┬───────────────┘
                ▼
        final dataset (CSV)
```

### `pipeline.py` — the orchestrator

`pipeline.py` runs the whole collection process for **12 files** (6 months × comments and submissions) and is designed for **two collaborators working on different machines**.

- **Parallel extraction.** Several `.zst` files are extracted at the same time with a `ThreadPoolExecutor`; each file is processed by the extractor in its own subprocess.
- **Role-based workload split.** Each machine has a role, and each role is responsible for its own months (see [Roles](#roles)). The other collaborator's months are not extracted locally; the pipeline just waits until the finished CSVs are placed in `output/`.
- **Role-based hardware profile.** Number of parallel workers and chunk size are chosen for the machine's RAM.
- **Waits for downloads.** A file is only picked up once it is fully downloaded (no torrent temp file such as `.!qB`, and the file size is stable).
- **Disk-space guard.** Extraction pauses if free space drops below `MIN_FREE_GB` (15 GB by default). Source `.zst` files can be deleted right after successful extraction (`DELETE_SOURCE_AFTER_EXTRACT`).
- **Resumable.** Progress is stored in `pipeline_state.json`, so the script can be stopped and restarted without redoing finished files. Existing output CSVs are skipped.
- **Logging.** Everything is written to `pipeline.log` and to the console.
- **Automatic merge.** When all 12 files are accounted for, the pipeline merges them into `final_dataset.csv`.

### Keyword filter used in the merge step

Text columns (`body` for comments; `title` and `selftext` for submissions) are matched case-insensitively against:

```text
ai|artificial intelligence|chatgpt|openai|claude|gpt|replaced|obsolete|layoff|automate|useless|takeover
```

A row is kept if **at least one** text column matches.

### Cleaning

The cleaning script (`clean_heuristic.py`) removes:

- bot comments (e.g. `AutoModerator`),
- deleted / removed posts,
- exact duplicates.

---

## Repository structure

```text
.
├── pipeline.py                      # Orchestrator: parallel extraction, state, merge
├── reddit_zst_filter_zstandard.py   # Extractor, pure Python (used by the pipeline)
├── reddit_zst_filter_zstd_jq.py     # Extractor, shell pipes (low-RAM alternative)
├── reddit_filter_utils.py           # Shared helpers for both extractors
├── clean_heuristic.py               # Post-processing: bots, deleted posts, duplicates
├── config.json                      # Global settings for the extractors
├── requirements.txt                 # Python dependencies
├── media/                           # Supporting media files
├── LICENSE
└── README.md
```

Files created at runtime (git-ignored): `data/`, `output/`, `logs/`, `pipeline.log`, `pipeline_state.json`, `role.local.txt`, `final_dataset.csv`.

---

## Installation

**Requirements:** Python 3.10+

```bash
git clone https://github.com/IlliaMartyniuk/reddit-dump-extractor.git
cd reddit-dump-extractor

python -m venv venv
# Windows:      venv\Scripts\activate
# macOS/Linux:  source venv/bin/activate

pip install -r requirements.txt
```

Main dependencies: `pandas`, `pyarrow`, `zstandard`, `orjson`, `psutil`.

> **Note.** `pipeline.py` opens each extraction in a separate console window using `subprocess.CREATE_NEW_CONSOLE`, which exists **only on Windows**. On macOS/Linux, remove the `creationflags=...` argument in `process_own_file()` or run the extractors directly.

The shell-pipe extractor (`reddit_zst_filter_zstd_jq.py`) additionally needs `zstd`, `jq` and `gsplit` (from coreutils):

```bash
# macOS
brew install zstd coreutils jq
# Linux
sudo apt install zstd coreutils jq
# Windows: use WSL, then the Linux command above
```

---

## Quick start (full pipeline)

### 1. Download the dumps

Download the monthly dumps from [Academic Torrents](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1) (for example with qBittorrent) and put them into:

```text
data/reddit/comments/      RC_2023-11.zst, RC_2024-04.zst, ...
data/reddit/submissions/   RS_2023-11.zst, RS_2024-04.zst, ...
```

You only need the months assigned to your role.

### 2. Set your role

Create `role.local.txt` next to `pipeline.py` containing just `me` or `partner`:

```bash
echo me > role.local.txt
```

The file is git-ignored, so two collaborators pushing to the same repo never overwrite each other's role. The pipeline exits with an error if the file is missing.

#### Roles

| Role | Extracts locally | Workers | Chunk size | Intended for |
| --- | --- | :---: | ---: | --- |
| `me` | 2023-11, 2024-04, 2024-11 | 6 | 500,000 | powerful machine (32 GB RAM, 14 cores) |
| `partner` | 2022-04, 2022-12, 2023-04 | 3 | 250,000 | lighter machine (16 GB RAM) |

Adjust `MONTHS_BY_PERSON`, `MAX_CONCURRENT_FILES` and `CHUNK_SIZE_STR` in `pipeline.py` to match your setup and team.

### 3. Exchange CSVs with your teammate

Extraction produces one CSV per input file in `output/` (for example `output/RC_2023-11.csv`). Copy the CSVs for the months you did **not** process into your own `output/` folder. The pipeline detects them automatically.

### 4. Run

```bash
python pipeline.py
```

The script polls the folders (every 60 seconds) until all 12 files are accounted for, then writes **`final_dataset.csv`**. Finally, run the cleaning step:

```bash
python clean_heuristic.py
```

### Pipeline settings

| Setting (in `pipeline.py`) | Default | Purpose |
| --- | --- | --- |
| `SUBREDDITS` | the 10 target communities | Subreddits kept during extraction |
| `MIN_FREE_GB` | `15` | Pause extraction if free disk space is lower |
| `DELETE_SOURCE_AFTER_EXTRACT` | `True` | Delete the `.zst` after successful extraction |
| `POLL_INTERVAL_SEC` | `120` | How often to re-check folders |

---

## Using the extractors on their own

The two extractors are general-purpose tools for filtering Reddit dumps by any field. Pick one depending on your resources:

| Method | File | Technology | RAM | Best for |
| --- | --- | --- | --- | --- |
| `zstandard` (Python) | `reddit_zst_filter_zstandard.py` | Python | High (8 GB+) | Speed, error tracking |
| `zstd_jq` (shell pipes) | `reddit_zst_filter_zstd_jq.py` | `zstd` + `jq` + `gsplit` | Very low (2–4 GB) | Huge files, limited RAM |

### Usage

```bash
python reddit_zst_filter_zstandard.py <input_folder> [options]
python reddit_zst_filter_zstd_jq.py   <input_folder> [options]
```

| Option | Description | Default |
| --- | --- | --- |
| `--output_dir` | Output directory | `output` |
| `--format` | `parquet` or `csv` | `csv` |
| `--field` | Field to filter on | `subreddit` |
| `--value` | Values to match (comma-separated) | `ukraine` |
| `--regex` | Treat values as regex patterns | `false` |
| `--file_filter` | Regex for input file names | `^RC_\|^RS_` |
| `--chunk_size` | Lines per chunk (`zstd_jq` only; `pipeline.py` also passes it to the Python extractor) | `1000000` |
| `--config` | Path to `config.json` | `config.json` |

### Examples

```bash
# The subreddits used in this project (comments and submissions)
python reddit_zst_filter_zstandard.py data/reddit/comments \
  --value "cscareerquestions,jobs,recruitinghell,copywriting,graphic_design,ArtistLounge,ChatGPT,artificial,singularity,Futurology"

# Regex search in the text of comments
python reddit_zst_filter_zstandard.py data/reddit/comments \
  --field body --value "layoff|obsolete" --regex

# Only comments from one month
python reddit_zst_filter_zstandard.py data/reddit/comments \
  --file_filter "^RC_2023-11"

# Low-RAM mode with smaller chunks, Parquet output
python reddit_zst_filter_zstd_jq.py data/reddit/comments \
  --chunk_size 500000 --format parquet --output_dir parquet_output
```

### Choosing `--chunk_size` (`zstd_jq`)

| Chunk size | Suggested RAM |
| ---: | --- |
| 250,000 | ~2 GB |
| 500,000 | 3–4 GB |
| 1,000,000 (default) | 4–8 GB |
| 2,000,000 | 8 GB+ |

---

## Output format

Each input `.zst` file produces its own file in `output/`, named after the input (for example `RC_2023-11.csv`).

The merged `final_dataset.csv` keeps only the columns needed for analysis (columns that do not exist for a given type are empty):

| Column | Description |
| --- | --- |
| `id` | Unique identifier of the comment / submission |
| `created_utc` | Creation timestamp (UTC, Unix time) |
| `subreddit` | Subreddit name |
| `author` | Author name |
| `score` | Number of votes |
| `body` | Text of a **comment** |
| `title` | Title of a **submission** |
| `selftext` | Text of a **submission** |

Extraction logs (memory/CPU usage, matched records per file, processing rate) are written to `logs/reddit_filter.log`.

---

## Troubleshooting

| Problem | What to do |
| --- | --- |
| `Missing role.local.txt` on start | Create the file with `me` or `partner` inside (see [Set your role](#2-set-your-role)). |
| `pipeline.py` keeps saying "still downloading" | The `.zst` is missing, still being downloaded, or in the wrong folder (`RC_` → `comments/`, `RS_` → `submissions/`). |
| Extraction is paused | Free disk space is below `MIN_FREE_GB`. Free some space or lower the threshold. |
| Out-of-memory errors | Lower `CHUNK_SIZE_STR` / `MAX_CONCURRENT_FILES`, or use the `zstd_jq` extractor. |
| `Missing N CSV files for the final merge` | Some months are not in `output/` yet — usually the teammate's CSVs. |
| `zstd_jq` returns empty results | Check that `jq` and `gsplit` are installed: `which jq gsplit`. |
| `AttributeError: CREATE_NEW_CONSOLE` | You are not on Windows — see the note in [Installation](#installation). |

---

## Links

- **Dataset (final CSV):** _add link here_
- **Report:** Homework #2 — Dataset Collection and Understanding

---

## Authors and credits

**Authors** — 2nd-year bachelor's students, Applied Mathematics, NaUKMA:

- [Illia Martyniuk](https://github.com/IlliaMartyniuk)
- Kseniia Hunaza

**Based on**

- [Watchful1](https://github.com/Watchful1) — author of [PushshiftDumps](https://github.com/Watchful1/PushshiftDumps/tree/master/scripts)
- [SanGreel/reddit-dump-extractor](https://github.com/SanGreel/reddit-dump-extractor) — the original restructured extractor this repository is forked from
- Pushshift team — for archiving and providing access to the Reddit dataset

Prepared as part of the **Computational Social Science (CSS)** course, National University of Kyiv-Mohyla Academy, 2026.

## License

See [LICENSE](LICENSE).
