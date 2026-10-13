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


def drop_duplicate_rows(df):
    """Drop rows repeated on (base_url, url, content).

    The spider already writes each (base_url, content) once, so this normally removes
    almost nothing. It exists for the resume case: the dedup ledger there is in-memory
    and starts empty, so a re-crawled url writes its row a second time. Keying on the
    url as well makes this a no-op for any genuinely distinct page, so it can only ever
    remove literal repeats of one url's text.

    Content is hashed before the comparison. A 64-bit hash of the full string is a
    fixed 8 bytes per row instead of the ~3.4 KB the string averages, which is what
    makes this affordable over a multi-million-row aggregate; the hash is only ever
    used to find candidate repeats, never stored.
    """
    if 'content' not in df.columns or 'base_url' not in df.columns:
        return df, 0
    # Empty content is kept regardless: it records a url that failed to hydrate, and
    # every such url is a distinct observation.
    non_empty = (df['content'].astype('bool') & df['content'].str.strip().ne('')).values
    if not non_empty.any():
        return df, 0

    key_cols = ['base_url', 'content']
    if 'url' in df.columns:
        key_cols.append('url')
    hashes = pd.util.hash_pandas_object(df['content'], index=False)
    keys = pd.DataFrame({'base_url': df['base_url'].values, 'h': hashes.values})
    if 'url' in df.columns:
        keys['url'] = df['url'].values

    # .values, not the Series: keys was built from plain arrays and so carries a fresh
    # RangeIndex, which would reindex-align against a source frame with any other index
    # and silently drop the wrong rows.
    dup = (keys.duplicated(keep='first') & non_empty).values
    dropped = int(dup.sum())
    if dropped:
        df = df[~dup]
    return df, dropped


# Yields all .parquet frames in a dir (and its subdirs)
def read_parquet_dir(parquet_dir, exclude=()):
    for root, dirs, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet') and file not in exclude:
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_parquet(file_path)
                except Exception:
                    continue
                if 'content' in df.columns:
                    yield df[df['content'].apply(is_valid_string)]
                else:
                    yield df
