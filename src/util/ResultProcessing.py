import logging
import os
import re

import pandas as pd


# Pre-compiled for hot path
_STRANGE_RE = re.compile(r'[\x00-\x08\x0B\x0E-\x1F\x7F]')


def is_valid_string(s):
    if not isinstance(s, str):
        return False
    if not s:
        return False
    # Count strange chars without building full list
    cnt = len(_STRANGE_RE.findall(s))
    return not (cnt / len(s)) > 0.1


# Yields all .parquet frames in a dir (and its subdirs)
def read_parquet_dir(parquet_dir, exclude=(), collected=None):
    for root, dirs, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet') and file not in exclude:
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_parquet(file_path)
                except Exception:
                    continue
                if collected is not None:
                    collected.append(file_path)
                if 'content' in df.columns:
                    yield df[df['content'].apply(is_valid_string)]
                else:
                    yield df


def dedupe_results(frame):
    """Keep one row per (base_url, url), preferring the most recent timestamp."""
    if frame is None or len(frame) == 0:
        return frame
    if "timestamp" in frame.columns:
        frame = frame.sort_values("timestamp", kind="stable")
    return frame.drop_duplicates(subset=["base_url", "url"], keep="last").reset_index(drop=True)


def build_aggregate(dir_results, aggregate_name, include_existing=True, aggregate_path=None):
    """Merge the existing aggregate with new batch parquet files.

    Returns ``(deduped_frame_or_None, raw_row_count, used_files)``. ``used_files``
    lists only batch files that read successfully, so callers can delete exactly
    what was merged and keep the crawl series cumulative.
    """
    frames = []
    base_path = aggregate_path or os.path.join(dir_results, aggregate_name)
    if include_existing and os.path.exists(base_path):
        try:
            frames.append(pd.read_parquet(base_path))
        except Exception as exc:
            logging.warning(f"Could not read existing aggregate {base_path}: {exc}")
    used_files = []
    frames.extend(
        read_parquet_dir(dir_results, exclude=(aggregate_name,), collected=used_files)
    )
    if not frames:
        return None, 0, used_files
    merged = pd.concat(frames, ignore_index=True)
    raw_rows = len(merged)
    return dedupe_results(merged), raw_rows, used_files


def write_aggregate(dir_results, aggregate_name, aggregate_path,
                    include_existing=True, cleanup=False):
    """Build, dedupe and write the aggregate; optionally delete merged batch files.

    Cleanup only runs after a successful write and only touches files that were
    read successfully. Returns ``(rows, raw_rows, deleted_files, freed_bytes)``.
    """
    results, raw_rows, used_files = build_aggregate(
        dir_results, aggregate_name,
        include_existing=include_existing, aggregate_path=aggregate_path,
    )
    if results is None or len(results) == 0:
        return 0, raw_rows, 0, 0
    print("#Results:", len(results))
    logging.info(
        f"Aggregate: {len(results)} rows after dedupe (raw {raw_rows}, "
        f"removed {raw_rows - len(results)})"
    )
    results.to_parquet(aggregate_path)
    deleted = 0
    freed = 0
    if cleanup and used_files:
        for path in used_files:
            try:
                size = os.path.getsize(path)
                os.remove(path)
                deleted += 1
                freed += size
            except OSError as exc:
                logging.warning(f"Could not delete aggregated batch file {path}: {exc}")
        logging.info(
            f"Aggregate cleanup: deleted {deleted} batch files "
            f"({freed / (1024 * 1024):.1f} MiB freed)"
        )
    return len(results), raw_rows, deleted, freed
