import urllib.request

from tempfile import NamedTemporaryFile
from typing import Iterator, Tuple, Type
from zipfile import ZipFile

import pandas as pd


def get_column_python_types(df: pd.DataFrame) -> Iterator[Tuple[str, Type]]:
    """Given a DataFrame, it generates the Python datatypes of the first row without null or NaN cells."""
    for column, value in df[~df.isna()].iloc[0, :].to_dict().items():
        yield column, type(value)


def fetch_from_zip_read_csv(
        uri: str,
        filename: str,
        *args,
        **kwargs
) -> pd.DataFrame:
    """Extract a DataFrame from a zip file hosted remotely"""
    with NamedTemporaryFile() as fp:
        urllib.request.urlretrieve(
            uri,
            fp.name
        )

        with ZipFile(fp.name) as zf:
            with zf.open(filename) as f:
                df = pd.read_csv(f, *args, **kwargs)

    return df
