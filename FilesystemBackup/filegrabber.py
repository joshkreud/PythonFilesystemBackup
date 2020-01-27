"""Pathsync
"""
from datetime import datetime
import concurrent.futures
import shutil
import os
import logging
from pathlib import Path
import pandas as pd
import tqdm.auto as tqdm


class TqdmLoggingHandler(logging.Handler):
    """Handler to get logging and TQDM to work nicely"""

    def emit(self, record):
        msg = self.format(record)
        try:
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            pass


def get_files(path: Path) -> pd.DataFrame:
    """Get all files in path into dataframe

    Arguments:
        path {Path} -- StartPath

    Returns:
        pd.DataFrame -- Dataframe['File','ModDate']
    """
    all_files = []
    LOGGER.info(f'Getting all Paths below: {path}')
    for i in tqdm.tqdm(path.glob("**/*"), unit=' paths', desc='Collecting'):
        if i.is_file():
            #!Deal with Weird Charmaps
            LOGGER.debug(f"Collecting File: {i}")
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
                f"Couldn't copy '{source}' to '{dest}' Error: {fne.strerror}")

    executor: concurrent.futures.Executor
    LOGGER.info(f"Copying {len(data_frame.index)} files.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Process the list of files, but split the work across the process pool to use all CPUs!
        for _ in tqdm.tqdm(executor.map(copy_file, data_frame[from_col], data_frame[to_col]),
                           total=len(data_frame.index), unit=' files', desc='Copying'):
            pass
    LOGGER.info("Finished copying")


def get_files_filtered(source_path: Path,
                       target_path: Path, cutoff_date: datetime) -> pd.DataFrame:
    """get files and Prepare the filter matrix

    Arguments:
        source_path {Path} -- where to get the files
        target_path {Path} -- where to move the files
        cutoff_date {datetime} -- only files after that date

    Returns:
        pd.DataFrame -- filtered dataframe
    """
    all_files = get_files(source_path)
    filtered_files = all_files.loc[all_files["Modified"]
                                   > cutoff_date, :].copy()

    filtered_files = filtered_files[~filtered_files["File"].map(str).str.contains(
        "_gsdata_", na=False)]
    LOGGER.info(
        f"Filtered Files: {len(filtered_files.index)} Overall Files: {len(all_files.index)}")
    filtered_files["NewPath"] = filtered_files["File"].apply(
        lambda p: target_path / p.relative_to(source_path)
    )
    return filtered_files


def copy_data(source_path: Path, target_path: Path, cutoff_date: datetime):
    """Gets files, filters out gsdata and files older than cutoff, copies the files to target
    """

    filtered_files = get_files_filtered(source_path, target_path, cutoff_date)
    if filtered_files.shape[0] > 0:
        for _, row in filtered_files[filtered_files.File.map(str).
                                     str.len() > 255].iterrows():
            LOGGER.warning(f'To long Source filePath: {row.File}')
        for _, row in filtered_files[filtered_files.NewPath.map(str).
                                     str.len() > 255].iterrows():
            LOGGER.warning(f'To long Target filePath: {row.File}')

        if os.path.exists(target_path):
            shutil.rmtree(target_path)
        if not os.path.exists(target_path):
            LOGGER.info(f"Creating: {target_path}")
            os.makedirs(target_path)
        copy_threaded(filtered_files, "File", "NewPath")
    else:
        LOGGER.info(f"No files changed since: {cutoff_date}")


LOGGER = logging.getLogger(__name__)
FORMATTER = logging.Formatter('%(asctime)s -%(name)s [%(levelname)s] %(message)s',
                              datefmt='%Y/%m/%d %H:%M:%S')
TQDM_LOG_HANDLER = TqdmLoggingHandler(logging.INFO)
TQDM_LOG_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(TQDM_LOG_HANDLER)


def yes_or_no(question):
    while "the answer is invalid":
        reply = str(input(question+' (y/n): ')).lower().strip()
        if reply[0] == 'y':
            return True
        if reply[0] == 'n':
            return False
