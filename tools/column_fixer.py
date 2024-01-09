import pandas as pd

from logging import Logger
from typing import List


class ColumnFixer:
    """
    A class that handles fixes columns for the VoC Integration
    """

    def __init__(self, logger: Logger):
        """
        Constructor for the ColumnFixer class.

        Args:
        - logger (Logger): uses the logger for audit and debugging purposes

        Returns:
        - None
        """
        self.logger = logger

    def fix_dates(
        self, raw_frame: pd.DataFrame, date_columns: List[str]
    ) -> pd.DataFrame:
        """
        Fixes the dates by transforming the column to date time

        Args:
        - raw_frame (DataFrame): Data frame to be fixed
        - date_columns (List[str]): Columns to be fixed

        Returns:
        - DataFrame
        """
        temp_frame = raw_frame

        for column in date_columns:
            temp_frame[column] = pd.to_datetime(temp_frame[column], format="%Y-%m-%d")

        return temp_frame

    def fix_column_to_string(
        self, raw_frame: pd.DataFrame, columns: List[str]
    ) -> pd.DataFrame:
        """
        Fixes the column by transforming it to string type

        Args:
        - raw_frame (DataFrame): Data frame to be fixed
        - column (str): Column name to be made to string type

        Returns:
        - DataFrame
        """

        temp_frame = raw_frame

        for column in columns:
            temp_frame[column] = temp_frame[column].astype(str)

        return temp_frame

    def fix_column_to_float(
        self, raw_frame: pd.DataFrame, columns: List[str]
    ) -> pd.DataFrame:
        """
        Fixes the column by transforming it to float type

        Args:
        - raw_frame (DataFrame): Data frame to be fixed
        - column (str): Column name to be made to float type

        Returns:
        - DataFrame
        """

        temp_frame = raw_frame

        for column in columns:
            temp_frame[column] = temp_frame[column].astype(float)

        return temp_frame

    def fix_column_name_to_string(self, raw_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Fixes the column name by transforming it to string type

        Args:
        - raw_frame (DataFrame): Data frame to be fixed

        Returns:
        - DataFrame
        """

        temp_frame = raw_frame

        temp_frame.columns = [str(name) for name in temp_frame.columns]

        return temp_frame

    def fix_column_remove_decimals(
        self, raw_frame: pd.DataFrame, col_name: str
    ) -> pd.DataFrame:
        """
        Fixes the column and makes it into a string

        Args:
        - raw_frame (DataFrame): Data frame to be fixed
        - col_name (str): column to be fixed

        Returns:
        - DataFrame
        """

        temp_frame = self.fix_column_to_string(raw_frame, [col_name])

        for index, row in temp_frame.iterrows():
            if "." in row[col_name]:
                temp_frame.loc[index, col_name] = row[col_name].split(".")[0]

        return temp_frame
