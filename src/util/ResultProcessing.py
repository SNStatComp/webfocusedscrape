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
def read_parquet_dir(parquet_dir):
    for root, dirs, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_parquet(file_path)
                except Exception:
                    continue
                if 'content' in df.columns:
                    yield df[df['content'].apply(is_valid_string)]
                else:
                    yield df
