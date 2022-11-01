import logging
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

import click
import pandas as pd

logger = logging.getLogger("__name__")


@click.argument("folder_with_csvs", type=Path)
@click.option("--store_path", type=Path)
@click.option("--certain_files", "-cf", type=str, multiple=True)
def _concatenate_cvs(folder_with_csvs: Path,
                     store_path: Path = None,
                     certain_files: Tuple[str] = ())\
        -> Tuple[List[Dict], Set]:
    """
    Method that goes through files in folder_with_csv and concatenates them into one
    list of dicts without duplications
    :param folder_with_csvs: folder where .csv files are stored
    :param store_path: path where to save resulting list as .csv file
    :param certain_files: tuple of certain .csv files which should be viewed
    :return: list of dicts, set of overviewed branch names
    """

    df = None
    branches = set()

    for filename in os.listdir(folder_with_csvs):
        if not filename.endswith(".csv"):
            continue
        if len(certain_files) != 0 and filename not in certain_files:
            continue
        branches.add("_".join(filename.split("_")[1:]).replace(".csv", ""))

        if df is None:
            df = pd.read_csv(folder_with_csvs / filename, index_col=0)
        else:
            df = pd.concat([df, pd.read_csv(folder_with_csvs / filename, index_col=0)])\
                .drop_duplicates()\
                .reset_index(drop=True)

    if store_path is not None:
        df.to_csv(store_path)

    result = df.to_dict("records")
    return result, branches


concatenate_cvs = click.command()(_concatenate_cvs)

if __name__ == "__main__":
    concatenate_cvs()
