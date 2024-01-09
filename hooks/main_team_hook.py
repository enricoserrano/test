from datetime import datetime
from typing import List

from ezyvet.data.models.hook_model import HookModel
from ezyvet.data.models.voc_variables import EMPTY_STRING_VAL


class MainTeamHook(HookModel):
    """
    Class that connects to the snowflake teamwork db
    """

    def retrieve_database(self) -> str:
        """
        Provides the database for the hook

        Returns:
        - str
        """

        return "VSSANALYTICS_DB"

    def retrieve_schema(self) -> str:
        """
        Provides the schema for the hook

        Returns:
        - str
        """

        return "TEAMWORK"

    def retrieve_table(self) -> str:
        """
        Provides the table name for the hook

        Returns:
        - str
        """

        return "cdl_teamwork"

    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """

        columns = [
            "main.PROJECT_ID",
            "main.SAP_ID",
            "main.PROJECT_NAME",
            "main.PROJECT_OWNER",
            "main.SURVEY_CONTACT_FIRST_NAME",
            "main.SURVEY_CONTACT_LAST_NAME",
            "main.SURVEY_CONTACT_EMAIL",
            "main.SURVEY_CONTACT_PHONE",
            "main.COUNTRY_TAG",
            "main.CORPORATE_GROUP",
            "main.PRODUCT",
            "main.SUB_PRODUCT",
            "saas_fee.EXPENSES_COST",
            "SUM(main.EXPENSES_COST)",
            "main.MILESTONE_DEADLINE",
            "main.PROJECT_START_AT",
            "main.CATEGORY_NAME",
            "main.PROJECT_OWNER_EMAIL",
            "'2'",
            "'TEAMWORK'",
        ]

        return columns

    def retrieve_snowflake_csvmap(self) -> dict:
        """
        Provides the mapping for the snowflake and csv

        Syntax:
        SNOWFLAKE COLUMN : CSV COLUMN

        Returns:
        - dict
        """

        mapping = {
            "main.PROJECT_ID": "UNIQUE ID",
            "main.SAP_ID": "SAP ID",
            "main.PROJECT_NAME": "Project Name",
            "main.PROJECT_OWNER": "Team Lead / PM",
            "main.SURVEY_CONTACT_FIRST_NAME": "First Name",
            "main.SURVEY_CONTACT_LAST_NAME": "Last Name",
            "main.SURVEY_CONTACT_EMAIL": "Email",
            "main.SURVEY_CONTACT_PHONE": "Phone",
            "main.COUNTRY_TAG": "Region Country",
            "main.CORPORATE_GROUP": "Corporate Group",
            "main.PRODUCT": "Product",
            "main.SUB_PRODUCT": "Project Type",
            "SUM(main.EXPENSES_COST)": "Implementation Fee",
            "saas_fee.EXPENSES_COST": "SaaS Fee",
            "main.MILESTONE_DEADLINE": "Project Go Live date",
            "main.PROJECT_START_AT": "Project Start Date",
            "main.CATEGORY_NAME": "Lead implementer",
            "main.PROJECT_OWNER_EMAIL": "Team Lead / PM Email",
            "'2'": "# of Implementers",
            "'TEAMWORK'": "Record Origin",
        }

        return mapping

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        generated_date = datetime.now().date()

        # check if there is value in from and to dates
        date_exclusion = "TRUE"
        if self.custom_from == EMPTY_STRING_VAL or self.custom_to == EMPTY_STRING_VAL:
            date_exclusion = f"DATEDIFF(day, main.MILESTONE_DEADLINE, '{generated_date}') = {self.day_filter}"
        else:
            date_exclusion = f"main.MILESTONE_DEADLINE >= '{self.custom_from}' AND main.MILESTONE_DEADLINE <= '{self.custom_to}'"

        exclusions = [
            date_exclusion,
            "main.MILESTONE_COMPLETED = TRUE",
            """(
                LOWER(main.SUB_PRODUCT) LIKE '%conversion%'
                OR LOWER(main.SUB_PRODUCT) LIKE '%fresh%'
                OR (
                    (
                        LOWER(main.PROJECT_NAME) LIKE '%conversion - cornerstone%'
                        OR
                        LOWER(main.PROJECT_NAME) LIKE 'cornerstone - %conversion%'
                    )
                      AND LOWER(main.SUB_PRODUCT) NOT LIKE '%misc%'
                    )
                )""",
            "LOWER(main.expenses_name) NOT LIKE '%subscription%'",
            "main.PROJECT_STATUS = 'active'",
        ]

        return exclusions

    def retrieve_sap_id_column(self) -> str:
        """
        Provides the sap id column name for the query

        Returns:
        - str
        """

        return "SAP_ID"

    def retrieve_join_tables(self) -> dict:
        """
        Provides the tables to be joined for the query

        Syntax:
        {"table name:alias name": "left join bool:join conditions"}

        Returns:
        - dict
        """

        table_joins = {
            "cdl_teamwork:saas_fee": """True:main.PROJECT_ID = saas_fee.PROJECT_ID
                                        AND LOWER(saas_fee.expenses_name) LIKE '%subscription%'
                                        AND (
                                            LOWER(saas_fee.SUB_PRODUCT) LIKE '%conversion%'
                                            OR LOWER(saas_fee.SUB_PRODUCT) LIKE '%fresh%'
                                            OR (LOWER(saas_fee.PROJECT_NAME) LIKE '%conversion - cornerstone%' AND LOWER(saas_fee.SUB_PRODUCT) NOT LIKE '%misc%')
                                        )"""
        }

        return table_joins

    def retrieve_group_by(self) -> bool:
        """
        Provides the tables to be joined for the query

        Returns:
        - bool
        """

        return True
