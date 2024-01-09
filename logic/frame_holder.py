import pandas as pd

from logging import Logger
from typing import Optional, List

from ezyvet.data.logic.snowflake_reader import SnowflakeReader
from ezyvet.data.tools.column_fixer import ColumnFixer

# imports to read the previous failed records
from ezyvet.data.models.voc_variables import (
    EMPTY_STRING_VAL,
    VOC_JOIN_UNIQUE_ID,
    DUPLICATED_RAW_COLUMNS,
)

# import the hooks
from ezyvet.data.models.hook_model import HookModel
from ezyvet.data.hooks.main_maven_hook import MainMavenHook
from ezyvet.data.hooks.main_team_hook import MainTeamHook
from ezyvet.data.hooks.supp_dx_hook import SuppDxHook
from ezyvet.data.hooks.supp_sap_hook import SuppSapHook
from ezyvet.data.hooks.supp_vdc_hook import SuppVDCHook
from ezyvet.data.hooks.supp_chargebee_hook import SuppChargeBeeHook
from ezyvet.data.hooks.supp_dxfsr_hook import SuppDXFSRHook
from ezyvet.data.hooks.main_mavenprev_hook import MainMavenPrevHook
from ezyvet.data.hooks.main_teamprev_hook import MainTeamPrevHook


class FrameHolder:
    """
    A class that retrieves the data frames from snowflake to be used for the VOC Integration
    """

    def __init__(self, logger: Logger):
        """
        Constructor for the FrameHolder class.

        Args:
        - logger (Logger): uses the logger for audit and debugging purposes

        Returns:
        - None
        """
        self.logger = logger
        self.base_frame: pd.DataFrame = None
        self.supplemental_frames: List[pd.DataFrame] = []

    def return_base_hooks(self) -> List[HookModel]:
        """
        Returns the hooks to be used as the base for the integration

        Returns:
        - List[HookModel]
        """
        return [
            MainMavenHook(),  # Mavenlink
            MainTeamHook(),  # Teamwork
        ]

    def return_supplemental_hooks(self) -> List[HookModel]:
        """
        Returns the hooks to be used to supplement data

        Returns:
        - List[HookModel]
        """
        return [
            SuppDxHook(),  # DX ARR
            SuppSapHook(),  # SAP
            SuppVDCHook(),  # VDC
            SuppDXFSRHook(),  # DX FSR
            SuppChargeBeeHook(),  # Chargebee / Neo Users
        ]

    def return_previous_base_hooks(self) -> dict:
        """
        Returns the hooks to grab previous failed records
        Syntax - RECORD ORIGIN : HOOK

        Returns:
        - dict[str, HookModel]
        """
        return {"MAVENLINK": MainMavenPrevHook(), "TEAMWORK": MainTeamPrevHook()}

    def retrieve_previous_fail_frame(
        self, snowReader: SnowflakeReader, failed_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Retrieves the records from the previous execution
        This is limited to the records on the immediate previous date

        Args:
        - snowReader (SnowflakeReader): Class that uses the hook model
        - df (DataFrame): Previous Failure Records DataFrame

        Returns:
        - DataFrame
        """
        # prepare the history hooks
        fixer = ColumnFixer(self.logger)
        existing_uniqueids = self.base_frame[VOC_JOIN_UNIQUE_ID].to_list()
        previous_frame = pd.DataFrame()
        for origin, hook in self.return_previous_base_hooks().items():
            temp_failed_entries = failed_df[
                (failed_df["Record Origin"] == origin)
            ].copy()
            self.logger.info("modifying unique ids column as string")
            temp_failed_entries = fixer.fix_column_remove_decimals(
                temp_failed_entries, VOC_JOIN_UNIQUE_ID
            )
            failed_uniqueids = temp_failed_entries[VOC_JOIN_UNIQUE_ID].to_list()
            self.logger.info("filtered failed sap id list")
            self.logger.info(failed_uniqueids)
            # if the unique ids is already present in the current valid entries, we skip
            hook.unique_ids = list(set(failed_uniqueids) - set(existing_uniqueids))
            if not hook.unique_ids:
                continue
            temp_frame = snowReader.get_dataframe_from_snowflake(hook)
            previous_frame = pd.concat([previous_frame, temp_frame], ignore_index=True)

        return previous_frame

    def retrieve_base_frame(
        self,
        previousFailedDataFrame: pd.DataFrame = pd.DataFrame(),
        custom_filename: str = EMPTY_STRING_VAL,
    ) -> Optional[None]:
        """
        Retrieves the base data frame from snowflake

        Args:
        - previousFailedDataFrame (DataFrame): Dataframe that contains the previous failed records
        - custom_filename (str): Used to grab the from and to dates for custom files

        Returns:
        - Optional[None]
        """
        if self.base_frame:
            return None

        # retrieve current valid entries
        snowReader = SnowflakeReader(self.logger)
        for hook in self.return_base_hooks():
            if custom_filename != EMPTY_STRING_VAL:
                daterange = custom_filename.split("_")
                hook.custom_from = daterange[1]
                hook.custom_to = daterange[2]
            # check if base_frame is empty
            if self.base_frame is None:
                self.base_frame = self.remove_duplicates_from_frames(
                    snowReader.get_dataframe_from_snowflake(hook)
                )
            else:
                temp_frame = self.remove_duplicates_from_frames(
                    snowReader.get_dataframe_from_snowflake(hook)
                )
                self.base_frame = pd.concat(
                    [self.base_frame, temp_frame], ignore_index=True
                )

        if not (previousFailedDataFrame.empty or custom_filename != EMPTY_STRING_VAL):
            temp_previous_frame = self.remove_duplicates_from_frames(
                self.retrieve_previous_fail_frame(snowReader, previousFailedDataFrame)
            )
            self.base_frame = pd.concat(
                [self.base_frame, temp_previous_frame], ignore_index=True
            )

        self.logger.info(self.base_frame.to_markdown())

    def retrieve_supplemental_frames(self, sap_ids: List[str]) -> Optional[None]:
        """
        Retrieves the supplemental data frames from snowflake

        Args:
        - None

        Returns:
        - Optional[None]
        """
        if self.supplemental_frames:
            return None

        snowReader = SnowflakeReader(self.logger)
        for hook in self.return_supplemental_hooks():
            # make sure that the sap_ids are being included in the hook
            hook.sap_ids = sap_ids
            self.supplemental_frames.append(
                snowReader.get_dataframe_from_snowflake(hook)
            )

    def remove_duplicates_from_frames(
        self, raw_frame: pd.DataFrame, filter_cols: List[str] = DUPLICATED_RAW_COLUMNS
    ) -> pd.DataFrame:
        """
        Removes duplicates from provided dataframe and choses the one with the most data in columns

        Args:
        - raw_frame (pd.DataFrame): frame with duplicates

        Returns:
        - pd.DataFrame
        """

        cf = ColumnFixer(self.logger)

        duplicated_frame = cf.fix_column_to_string(
            (
                raw_frame[raw_frame.duplicated(filter_cols)][filter_cols]
                .copy()
                .drop_duplicates(keep="first")
            ),
            filter_cols,
        )

        if duplicated_frame.empty:
            return raw_frame

        self.logger.info("Raw Records")
        self.logger.info(raw_frame.to_json())

        self.logger.info("Duplicated Records")
        self.logger.info(duplicated_frame.to_json())

        final_frame = raw_frame.copy()
        temp_frame = cf.fix_column_to_string(raw_frame.copy(), filter_cols)

        del_index = []  # index to delete
        for row_index, row in duplicated_frame.iterrows():
            dynamic_conditions = []

            # generate query
            for col in filter_cols:
                dynamic_conditions.append(f"`{col}` == '{row[col]}'")
            query = f" & ".join(dynamic_conditions)

            # grab all of the items with duplicated columns listed in filter_cols
            col_checker = temp_frame.query(query).copy()
            col_checker["number_of_nans"] = (
                col_checker[col_checker.columns].isna().sum(1)
            )

            self.logger.info(query)
            self.logger.info(col_checker.to_json())

            retain_index = 0
            index_to_review = list(col_checker.index.values)
            number_of_nans = len(col_checker.columns)  # grab the number of columns
            for col_checker_index, row in col_checker.iterrows():
                if row["number_of_nans"] < number_of_nans:
                    retain_index = col_checker_index
                    number_of_nans = row["number_of_nans"]

            indices_to_remove = [
                index for index in index_to_review if index != retain_index
            ]

            del_index += indices_to_remove

        self.logger.info("Dropping Index")
        self.logger.info(del_index)
        final_frame = final_frame.drop(index=del_index)

        self.logger.info("Cleaned Records")
        self.logger.info(final_frame.to_json())

        return final_frame
