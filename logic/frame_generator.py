import copy
import pandas as pd

from logging import Logger
from typing import List

from ezyvet.data.models.voc_variables import (
    DATE_COLUMNS,
    FLOAT_COLUMNS,
    OLD_COMPARISON_COLUMNS,
    STRING_COLUMNS,
    VOC_JOIN_COLUMN,
    VOC_SURVEY_EXPORT,
    VOC_SURVEY_EXPORT,
    VOC_SURVEY_FAILED_EXPORT_ADDONS,
    VOC_REVENUE_FRAME_LIST,
    VOC_REVENUE_EXPORT,
    VOC_JOIN_UNIQUE_ID,
)
from ezyvet.data.tools.column_fixer import ColumnFixer


class FrameGenerator:
    """
    A class that handles the creation of frames
    """

    def __init__(self, logger: Logger):
        """
        Constructor for the Generate_Frame class.

        Args:
        - logger (Logger): uses the logger for audit and debugging purposes

        Returns:
        - None
        """
        self.logger = logger

    def merge_frames(
        self, base_frame: pd.DataFrame, supp_frames: List[pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Merges the frames by the join column horizontally

        Args:
        - base_frame (DataFrame): The base frame where the supplimental frames will be merged against
        - supp_frames (List[DataFrame]): List of frames to be merged against each other.

        Returns:
        - DataFrame
        """

        fixer = ColumnFixer(self.logger)
        fixed_base = fixer.fix_column_to_string(base_frame, [VOC_JOIN_COLUMN])

        for frame in supp_frames:
            # prepare and the frame to be merged
            frame = fixer.fix_column_to_string(frame, [VOC_JOIN_COLUMN])
            self.logger.debug(frame.to_markdown())

            fixed_base = fixed_base.merge(frame, on=VOC_JOIN_COLUMN, how="left")

        fixed_base = fixer.fix_column_to_string(fixed_base, STRING_COLUMNS)
        fixed_base = fixer.fix_column_to_float(fixed_base, FLOAT_COLUMNS)
        fixed_base = fixer.fix_dates(fixed_base, DATE_COLUMNS)
        fixed_base = fixer.fix_column_name_to_string(fixed_base)

        self.logger.info(fixed_base.to_markdown())

        return fixed_base

    def clean_frame(self, df: pd.DataFrame, columnList: List[str]) -> pd.DataFrame:
        """
        Returns a cleaned frame based from the list provided

        Args:
        - df (DataFrame): Data frame to be cleaned
        - columnList (List[str]) columns that will be retained

        Returns:
        - DataFrame
        """
        cleaned_frame = df.reindex(columns=columnList)

        self.logger.debug(df.columns.values.tolist())
        self.logger.debug(cleaned_frame.columns.values.tolist())
        return cleaned_frame

    def create_processed_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates the processed frame

        Args:
        - df (DataFrame): Where the new frame will base the data from

        Returns:
        - DataFrame
        """
        frame = copy.deepcopy(df)
        vdc_frame = copy.deepcopy(df)
        fsr_frame = copy.deepcopy(df)

        vdc_frame["First Name"] = vdc_frame["VDC_First_Name"]
        vdc_frame["Last Name"] = vdc_frame["VDC_Last_Name"]
        vdc_frame["Email"] = vdc_frame["VDC_Email"]
        vdc_frame["Phone"] = vdc_frame["VDC_Phone"]
        vdc_frame["Role"] = "VDC"

        fsr_frame["First Name"] = fsr_frame["DXFSR_First_Name"]
        fsr_frame["Last Name"] = fsr_frame["DXFSR_Last_Name"]
        fsr_frame["Email"] = fsr_frame["DXFSR_Email"]
        fsr_frame["Phone"] = fsr_frame["DXFSR_Phone"]
        fsr_frame["Role"] = "DX FSR"

        # append the vdc frame to the frame
        base_frame = pd.concat([frame, vdc_frame, fsr_frame], ignore_index=True)

        return base_frame

    def separate_valid_frames(
        self, df: pd.DataFrame, previous_failed_records: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Separate the valid data from the dataframe

        Args:
        - df (DataFrame): Where the new frame will base the data from
        - previous_failed_records(DataFrame): Contains the previous failed records.

        Returns:
        - List[DataFrame]
        """
        # Transform sap_id column as string before we process
        fixer = ColumnFixer(self.logger)
        vdf = fixer.fix_column_remove_decimals(df, VOC_JOIN_COLUMN)
        # Transform unique id column as string before we process to avoid duplication
        vdf = fixer.fix_column_to_string(vdf, [VOC_JOIN_UNIQUE_ID])

        # Grab entries with valid first name, last name, and email
        # VOC_JOIN_COLUMN is now a string type, valid frame should not be '1', '0', NULL, empty string ('') or sap_account_name should not be null
        valid_frame = vdf.loc[
            (
                (~vdf["First Name"].isnull())
                & (~vdf["Last Name"].isnull())
                & (~vdf["Email"].isnull())
                & (~vdf["Product"].isnull())
                & (~vdf["sap_account_name"].isnull())
                & (vdf[VOC_JOIN_COLUMN] != "1")
                & (vdf[VOC_JOIN_COLUMN] != "0")
                & (vdf[VOC_JOIN_COLUMN] != "")
                & (~vdf[VOC_JOIN_COLUMN].isnull())
            )
        ].copy()

        if not previous_failed_records.empty:
            # Transform sap_id column as string before processing previous dataframe
            fixed_failed_records = fixer.fix_column_remove_decimals(
                previous_failed_records.copy(), VOC_JOIN_COLUMN
            )

            # Don't reprocess records that have '0', '1', or empty string ('')
            nonempty_failed_records = fixed_failed_records[
                (~fixed_failed_records[VOC_JOIN_COLUMN].isnull())
                & (fixed_failed_records[VOC_JOIN_COLUMN] != "0")
                & (fixed_failed_records[VOC_JOIN_COLUMN] != "1")
                & (fixed_failed_records[VOC_JOIN_COLUMN] != "")
            ].copy()

            # DT-3058: clean up possible duplicates in failed records as a safe guard to avoid duplicating the reprocessed records
            saprole_failed_records = (
                nonempty_failed_records[OLD_COMPARISON_COLUMNS].copy().drop_duplicates()
            )
            # Transform unique id column as string before comparison
            saprole_failed_records = fixer.fix_column_to_string(
                saprole_failed_records, [VOC_JOIN_UNIQUE_ID]
            )

            failed_uniqueids = saprole_failed_records[VOC_JOIN_UNIQUE_ID].to_list()
            # log failed unique ids
            self.logger.info("Failed Records SAP IDs")
            self.logger.info(failed_uniqueids)

            valid_uniqueids = valid_frame[VOC_JOIN_UNIQUE_ID].to_list()
            self.logger.info("Failed Records SAP IDs")
            self.logger.info(valid_uniqueids)

            new_records = valid_frame[
                ~valid_frame[VOC_JOIN_UNIQUE_ID].isin(failed_uniqueids)
            ]

            temp_frame = copy.deepcopy(valid_frame)

            self.logger.info(new_records)
            self.logger.info(new_records.to_markdown())
            self.logger.info(saprole_failed_records.to_markdown())
            self.logger.info(temp_frame.to_markdown())

            # Transform both columns into a string before merging
            temp_frame = fixer.fix_column_to_string(temp_frame, OLD_COMPARISON_COLUMNS)

            saprole_failed_records = fixer.fix_column_to_string(
                saprole_failed_records, OLD_COMPARISON_COLUMNS
            )

            old_failed_records = temp_frame.merge(
                saprole_failed_records, on=OLD_COMPARISON_COLUMNS
            )

            self.logger.info(old_failed_records.to_markdown())

            revalidated_frame = pd.concat(
                [new_records, old_failed_records], ignore_index=True
            )
        else:
            revalidated_frame = valid_frame

        # always clean the frame to filter out unneeded columns
        cleaned_frame = self.clean_frame(revalidated_frame, VOC_SURVEY_EXPORT)

        return cleaned_frame

    def separate_invalid_frames(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Separate the invalid data from the dataframe

        Args:
        - df (DataFrame): Where the new frame will base the data from

        Returns:
        - DataFrame
        """
        # Transform sap_id column as string before we process
        fixer = ColumnFixer(self.logger)
        idf = fixer.fix_column_remove_decimals(df, VOC_JOIN_COLUMN)

        # Grab entries that were not grabbed by above
        # VOC_JOIN_COLUMN is now a string type, records are invalid if VOC_JOIN_COLUMN is '1', '0', NULL, empty string ('') or sap_account_name is null
        invalid_frame = idf.loc[
            (
                (idf["First Name"].isnull())
                | (idf["Last Name"].isnull())
                | (idf["Email"].isnull())
                | (idf["Product"].isnull())
                | (idf["sap_account_name"].isnull())
                | (idf[VOC_JOIN_COLUMN].isnull())
                | (idf[VOC_JOIN_COLUMN] == "0")
                | (idf[VOC_JOIN_COLUMN] == "1")
                | (idf[VOC_JOIN_COLUMN] == "")
            )
        ]

        failed_records_columns = VOC_SURVEY_EXPORT + VOC_SURVEY_FAILED_EXPORT_ADDONS

        # always clean the frame to filter out unneeded columns
        cleaned_frame = self.clean_frame(invalid_frame, failed_records_columns)

        return cleaned_frame

    def create_revenue_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create the revenue frame based from the provided frame

        Args:
        - df (DataFrame): Where the new frame will base the data from

        Returns:
        - DataFrame
        """
        default_list = VOC_REVENUE_FRAME_LIST

        list_saas = copy.deepcopy(default_list)
        list_impl_fee = copy.deepcopy(default_list)
        list_cag_fee = copy.deepcopy(default_list)

        list_saas.append("SaaS Fee")
        list_impl_fee.append("Implementation Fee")
        list_cag_fee.append("IDEXX DX Spend")

        # saas fee revenues
        revenue_saas_frame = df[list_saas].copy()
        revenue_saas_frame["Revenue Type"] = "MRR"
        revenue_saas_frame["Description"] = "ezyvet/Neo/Cornerstone SaaS fee"
        revenue_saas_frame = revenue_saas_frame.rename(
            columns={"SaaS Fee": "Revenue Amount"}
        )

        # implementation fee revenues
        revenue_impl_frame = df[list_impl_fee].copy()
        revenue_impl_frame["Revenue Type"] = "One Time"
        revenue_impl_frame["Description"] = "ezyvet/Neo/Cornerstone Implementation fee"
        revenue_impl_frame = revenue_impl_frame.rename(
            columns={"Implementation Fee": "Revenue Amount"}
        )

        # cag fee revenue
        revenue_cag_frame = df[list_cag_fee].copy()
        revenue_cag_frame["Revenue Type"] = "CAG ARR"
        revenue_cag_frame["Description"] = "CAG Annual Recurring Revenue"
        revenue_cag_frame = revenue_cag_frame.rename(
            columns={"IDEXX DX Spend": "Revenue Amount"}
        )

        merged_frame = pd.concat(
            [revenue_saas_frame, revenue_impl_frame, revenue_cag_frame],
            ignore_index=True,
        )

        # set up the dates
        merged_frame["Revenue End Date"] = merged_frame["Project Go Live date"]
        merged_frame = merged_frame.rename(
            columns={"Project Go Live date": "Revenue Start Date"}
        )

        # always clean the frame to filter out unneeded columns
        cleaned_frame = self.clean_frame(merged_frame, VOC_REVENUE_EXPORT)
        return cleaned_frame
