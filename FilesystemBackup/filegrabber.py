"""Pathsync
"""
from datetime import datetime
import shutil
import os
import logging
from pathlib import Path
import pandas as pd

LOGGER = logging.getLogger(__name__)


def get_files(path: Path) -> pd.DataFrame:
    """Get all files in path into dataframe

    Arguments:
        path {Path} -- StartPath

    Returns:
        pd.DataFrame -- Dataframe['File','ModDate']
    """
    all_files = []
    LOGGER.info(f"Getting all Paths below: {path}")
    for idx, i in enumerate(path.glob("**/*")):
        if len(all_files) % 100 == 0:
            LOGGER.info(f"Collected so far: ({len(all_files)})")
        if i.is_file():
            #!Deal with Weird Charmaps
            LOGGER.debug(f"Collecting File: ({idx}) {i}")
            all_files.append((i, datetime.fromtimestamp(i.stat().st_mtime)))

    columns = ["File", "Modified"]
    LOGGER.info(f"Collected: {len(all_files)} files")
    return pd.DataFrame.from_records(all_files, columns=columns)


def copy_threaded(data_frame: pd.DataFrame, from_col: str, to_col: str):
    """Copy files from dataframe but threaded

    Arguments:
        data_frame {pd.DataFrame} -- input dataframe
        from_col {str} -- column with paths "From"
        to_col {str} -- column with paths "to"
    """

    def copy_file(source: Path, dest: Path):
        try:
            if not os.path.exists(dest.parent):
                try:
                    os.makedirs(dest.parent)
                except FileExistsError:
                    pass
            shutil.copyfile(source, dest)
        except FileNotFoundError as fne:
            LOGGER.warning(
                f"Couldn't copy '{source}' to '{dest}' Error: {fne.strerror}"
            )

    LOGGER.info(f"Copying {len(data_frame.index)} files.")
    for index, row in data_frame.iterrows():
        source = row[from_col]
        dest = row[to_col]
        mysizemb = round(source.stat().st_size / (1024 * 1024), 3)
        LOGGER.info(
            f'Copying: ({index}/{len(data_frame)}) ({mysizemb}mb) "{source.resolve()}" to "{dest.resolve()}"'
        )
        copy_file(source, dest)
    LOGGER.info("Finished copying")


def get_files_filtered(
    source_path: Path, target_path: Path, cutoff_date: datetime
) -> pd.DataFrame:
    """get files and Prepare the filter matrix

    Arguments:
        source_path {Path} -- where to get the files
        target_path {Path} -- where to move the files
        cutoff_date {datetime} -- only files after that date

    Returns:
        pd.DataFrame -- filtered dataframe
    """
    all_files = get_files(source_path)
    filtered_files = all_files.loc[all_files["Modified"] > cutoff_date, :].copy()

    filtered_files = filtered_files[
        ~filtered_files["File"].map(str).str.contains("_gsdata_", na=False)
    ]
    LOGGER.info(
        f"Filtered Files: {len(filtered_files.index)} Overall Files: {len(all_files.index)}"
    )
    filtered_files["NewPath"] = filtered_files["File"].apply(
        lambda p: target_path / p.relative_to(source_path)
    )
    return filtered_files


def copy_data(source_path: Path, target_path: Path, cutoff_date: datetime):
    """Gets files, filters out gsdata and files older than cutoff, copies the files to target
    """

    filtered_files = get_files_filtered(source_path, target_path, cutoff_date)
    if filtered_files.shape[0] > 0:
        for _, row in filtered_files[
            filtered_files.File.map(str).str.len() > 255
        ].iterrows():
            LOGGER.warning(f"To long Source filePath: {row.File}")
        for _, row in filtered_files[
            filtered_files.NewPath.map(str).str.len() > 255
        ].iterrows():
            LOGGER.warning(f"To long Target filePath: {row.File}")

        if os.path.exists(target_path):
            shutil.rmtree(target_path)
        if not os.path.exists(target_path):
            LOGGER.info(f"Creating: {target_path}")
            os.makedirs(target_path)
        copy_threaded(filtered_files, "File", "NewPath")
    else:
        LOGGER.info(f"No files changed since: {cutoff_date}")
