import copy
import math
import pandas as pd

from logging import Logger

from ezyvet.data.models.voc_variables import (
    TOUCHPOINT_DEFAULT_VALUE,
    ROLE_DEFAULT_VALUE,
)
from ezyvet.data.models.voc_maps import IMPLEMENTER_REGION_DICT, REGION_DICT


class ComputeFields:
    """
    A class that handles the computed fields for the VoC Integration
    """

    def __init__(self, logger: Logger):
        """
        Constructor for the ComputeFields class.

        Args:
        - logger (Logger): uses the logger for audit and debugging purposes

        Returns:
        - None
        """
        self.logger = logger

    def add_computed_fields(self, raw_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Adds the computed fields to the dataframe

        Args:
        - raw_frame (DataFrame): holds the raw csv dataframe

        Returns:
        - DataFrame
        """
        temp_frame = copy.deepcopy(raw_frame)

        self.logger.info(raw_frame)
        self.logger.info(temp_frame)

        # create an instance of the columns and make it to None instead of Nan
        temp_frame["Account Name - SAP ID"] = ""
        temp_frame["Touchpoint"] = ""
        temp_frame["Customer Tier"] = ""
        temp_frame["Role"] = ""
        temp_frame["Region"] = ""
        temp_frame["Locale"] = ""

        for index, row in temp_frame.iterrows():
            # generate account name:
            if row["sap_account_name"]:
                temp_frame.loc[index, "Account Name - SAP ID"] = (
                    row["sap_account_name"] + "-" + str(row["SAP ID"])
                )
            else:
                temp_frame.loc[index, "Account Name - SAP ID"] = (
                    "SAP_ACCOUNT_NAME_NOT_FOUND" + "-" + str(row["SAP ID"])
                )

            # generate touchpoint:
            temp_frame.loc[index, "Touchpoint"] = TOUCHPOINT_DEFAULT_VALUE

            # generate customer tier:
            if not row["Group"]:
                tier = "Individual"
            else:
                tier = "Enterprise"

            if not row["Country"]:
                temp_frame.loc[index, "Country"] = "XX"

            temp_frame.loc[index, "Customer Tier"] = tier

            # generate role:
            temp_frame.loc[index, "Role"] = ROLE_DEFAULT_VALUE

            # generate region
            temp_frame.loc[index, "Region"] = self.generate_region(
                row["Region Country"]
            )

            # generate locale:
            temp_frame.loc[index, "Locale"] = self.generate_locale(
                temp_frame.loc[index, "Region"]
            )

            # generate implementer office base:
            if not row["Implementer Office Base"]:
                temp_frame.loc[
                    index, "Implementer Office Base"
                ] = self.generate_implementer_office_base(
                    temp_frame.loc[index, "Team Lead / PM"]
                )

            # generate converted from if it doesnt exist. row is probably from teamwork
            if not row["Converted From"]:
                if "Conversion - " in row["Project Name"]:
                    guessConvertedFrom = (
                        row["Project Name"].split("Conversion - ")[1].split(")")[0]
                    )
                    temp_frame.loc[index, "Converted From"] = guessConvertedFrom

            # generate the user bracket if team usercount exists and it is not empty
            if "Team UserCount" in temp_frame.columns:
                if not pd.isnull(temp_frame.loc[index, "Team UserCount"]):
                    temp_frame.loc[index, "User Bracket"] = self.generate_user_bracket(
                        row["Team UserCount"]
                    )

            ## Recomputation of data to fix issues in Teamwork Data Pull
            # recompute the product if the value is MISC and if the project name contains Conversion - Cornerstone
            if row["Product"]:
                if (
                    "MISC" in row["Product"]
                    and "Conversion - Cornerstone" in row["Project Name"]
                ):
                    temp_frame.loc[index, "Product"] = "Cornerstone"

            # recompute the project type if the value is MISC and if the project name contains Conversion - Cornerstone
            if row["Project Type"]:
                if (
                    "MISC" in row["Project Type"]
                    and "Conversion - Cornerstone" in row["Project Name"]
                ):
                    temp_frame.loc[
                        index, "Project Type"
                    ] = "Cornerstone Conversion / Fresh"

        self.logger.debug(temp_frame)

        return temp_frame

    def generate_user_bracket(self, user_count: str) -> str:
        """
        Generates the user bracket if able to

        Args:
        - user_count (str): user count taken from teamwork

        Returns:
        - str
        """

        try:
            # try to cast the user count into integer
            conv_int = int(user_count)

            # if the casting is successful, continue with the if else maze to map the bracket.
            if conv_int < 1:
                raise ValueError("User Count has to be at least 1")
            elif conv_int == 1:
                return "1"
            elif conv_int < 10:
                return "2-9"
            elif conv_int < 20:
                return "10-19"
            elif conv_int < 30:
                return "20-29"
            elif conv_int < 50:
                return "30-49"
            elif conv_int < 75:
                return "50-74"
            elif conv_int >= 750:
                # we want to have 750+ be a catch all
                return "750+"
            else:
                roundedDown = math.floor(conv_int / 25)
                minValueRange = roundedDown * 25
                maxValueRange = ((roundedDown + 1) * 25) - 1
                return f"{minValueRange}-{maxValueRange}"

        except Exception as e:
            self.logger.info(
                f"""Cannot Determine User Bracket. Original Value: {user_count}
Error: {e}"""
            )
            return ""

    def generate_implementer_office_base(self, team_lead: str) -> str:
        """
        Generates the implementer office base

        Args:
        - team_lead (str): team lead of the record

        Returns:
        - str
        """

        if team_lead:
            ##implementer dictionary:
            impl_dict = IMPLEMENTER_REGION_DICT
            return impl_dict.get(team_lead, "NA")

        return ""

    def generate_locale(self, country: str) -> str:
        """
        Generates the locale for the client

        Args:
        - country (str): country of the record

        Returns:
        - str
        """

        ##country dictionary:
        ##locale_dict = LOCALE_DICT
        ##return locale_dict.get(country, "en_US")

        ## As per discussion in teams, locale triggers branching which we dont need at the moment
        return "en_US"

    def generate_region(self, country: str) -> str:
        """
        Generates the region for the client

        Args:
        - country (str): country of the record

        Returns:
        - str
        """
        region_dict = REGION_DICT

        return region_dict.get(country, "EMEA")
