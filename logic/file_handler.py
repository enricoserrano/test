import pandas as pd
import datetime as dt
from logging import Logger
from s3fs import S3FileSystem
from typing import List

from ezyvet.data.logic.frame_holder import FrameHolder
from ezyvet.data.models.voc_variables import (
    BUCKET,
    DEFAULT_FILE_PATH,
    EMPTY_STRING_VAL,
    INTEGRATION_NAME,
    VOC_NA_VALUES,
)


class FileHandler:
    """
    Class that contains logic with regards to file handling such as upload and read
    """

    def __init__(self, logger: Logger):
        self.logger = logger

    def read_csvfile_to_frame(self, path: str) -> pd.DataFrame:
        """
        Reads csv file and returns DataFrame

        Args:
        - path (str): path of the csv

        Returns:
        - pd.DataFrame
        """

        frame = pd.DataFrame()
        try:
            frame = pd.read_csv(
                path, dtype=object, na_values=VOC_NA_VALUES, keep_default_na=False
            )
        except Exception as e:
            self.logger.exception("Error occured while reading file")
            self.logger.exception(e)
            self.logger.exception(path)

        return frame

    def consolidate_dataframe(
        self,
        current_day: int,
        column_duplicate_filter: List[str],
        prefix: str,
        file_paths: List[str] = [],
        to_date: dt.date = dt.datetime.now(),
    ) -> pd.DataFrame:
        """
        Returns the consolidated dataframe based from the daterange provided.

        Consolidation only happens on Tuesdays and Thursdays for Reporting

        Args:
        - current_day (int): Day of the week in integer (0 - Sunday)
        - column_duplicate_filter (List[str]): columns that the duplicate check logic will use
        - prefix (str): contains the prefix to be used by the consolidation logic
        - file_paths (List[str]): file paths to be consolidated
        - to_date (dt.date): The last day included in the consolidated file. Default uses the current date

        Returns:
        - pd.DataFrame
        """

        lim = 0

        if current_day == 1:
            lim = 5
        elif current_day == 3:
            lim = 2
        else:
            message = "Current day is not included in mapping."
            self.logger.exception(message)
            exit(1)

        i = 0
        consolidated_frame = pd.DataFrame()

        # check if we have provided the filepaths in the function already
        if len(file_paths) == 0:
            # generate the file path of the previous executions
            while i < lim:
                date_loop = (to_date - dt.timedelta(days=i)).date()
                filename = self.dynamic_filename_generator(prefix, date_loop)
                dynamic_path = self.dynamic_folderpath_generator(date_loop)
                key = (
                    f"{DEFAULT_FILE_PATH}/{INTEGRATION_NAME}/{dynamic_path}/{filename}"
                )
                file_paths.append(f"s3://{BUCKET}/{key}")
                i += 1

        # read and consolidate the files to a singular dataframe
        for individual_path in file_paths:
            reader = self.read_csvfile_to_frame(individual_path)
            consolidated_frame = pd.concat(
                [consolidated_frame, reader],
                ignore_index=True,
            )

        if consolidated_frame.empty:
            return EMPTY_STRING_VAL

        # remove possible duplicates if there are any. Function also logs duplicates
        holder = FrameHolder(self.logger)

        finalised_frame = holder.remove_duplicates_from_frames(
            consolidated_frame, column_duplicate_filter
        )

        return finalised_frame

    def upload_frame_to_csv(self, path: str, frame: pd.DataFrame):
        """
        Uploads the dataframe as csv to provided s3 path

        Args:
        - path (str): upload path of the csv
        - frame (pd.DataFrame): dataframe to be uploaded
        """
        try:
            with S3FileSystem().open(path, "w") as fs:
                frame.to_csv(fs, index=False)
        except Exception as e:
            self.logger.exception("Error occured while uploading file")
            self.logger.exception(e)
            self.logger.exception(path)

    def dynamic_filename_generator(
        self, prefix: str, date: dt.date, file_extension: str = ".csv"
    ) -> str:
        """
        Returns the dynamic filename based from the provided details

        Args:
        - prefix (str): Prefix of the filename
        - date (dt.date): date for the filename
        - file_extension (str): file extension to be used. Default is .csv

        Returns:
        - str
        """

        return f"{prefix}_{date.strftime('%m.%d.%Y')}{file_extension}"

    def dynamic_folderpath_generator(
        self, date: dt.date, prefix: str = "", suffix: str = ""
    ) -> str:
        """
        Returns the dynamic folder path to be used to generate the key path

        Args:
        - date (dt.date): date for the folder path
        - prefix (str): prefix of the folder path
        - suffix (str): suffix for the folder path

        Returns:
        - str
        """

        paths: List[str] = []

        if prefix != "":
            paths.append(prefix)

        paths.append(str(date.year))
        paths.append(str(date.month))
        paths.append(str(date))

        if suffix != "":
            paths.append(suffix)

        return "/".join(paths)
