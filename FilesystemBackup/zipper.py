"""Module for zipping

"""
from pathlib import Path
import tqdm.auto as tqdm
import zipfile
import os
import logging

LOGGER = logging.getLogger(__name__)


def create_zip(target_path: Path, folder_to_zip: Path) -> Path:
    """Creates a Zip file of the specified folder

    Arguments:
        target_path {Path} -- where to put the Zip File
        folder_to_zip {Path} -- Which folder to zip?

    Returns:
        [type] -- Sucess Boolean
    """
    def get_all_file_paths(directory: Path) -> list:
        file_paths = []
        count = 0
        pbar = tqdm.tqdm(directory.rglob("*.*"))
        for i in pbar:
            if i.is_file():
                count += 1
                LOGGER.debug(f'collecting: {i}')
                file_paths.append(i)
        return file_paths

    zip_path = target_path.with_suffix('.zip')
    LOGGER.info(f"Zipping to file: {zip_path}")
    if os.path.exists(zip_path):
        LOGGER.info('Deleting preexisting Zip')
        os.remove(zip_path)
    all_files = get_all_file_paths(folder_to_zip)
    if not all_files:
        LOGGER.error(f'No files found to zip in: {folder_to_zip}')
        return False
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as myzip:
        for file in tqdm.tqdm(all_files, total=len(all_files), unit=' files', desc='Zipping'):
            LOGGER.info(f'Zipping file: {file}')
            myzip.write(file, file.relative_to(folder_to_zip))
    LOGGER.info("Zipping Done")
    return zip_path
