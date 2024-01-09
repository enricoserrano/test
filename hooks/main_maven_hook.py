import pytz

from datetime import datetime
from typing import List

from ezyvet.data.models.hook_model import HookModel
from ezyvet.data.models.voc_variables import EMPTY_STRING_VAL


class MainMavenHook(HookModel):
    """
    Class that connects to the snowflake mavenlink db
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

        return "MAVENLINK"

    def retrieve_table(self) -> str:
        """
        Provides the table name for the hook

        Returns:
        - str
        """

        return "cdl_mavenlink"

    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """

        columns = [
            "WORKSPACE_ID",
            "SAP_ID",
            "PROJECT_TITLE",
            "TEAM_LEAD",
            "TEAM_LEAD_EMAIL",
            "TEAM_LEAD_REGION",
            "LEAD_IMPLEMENTER",
            "COUNTRY",
            "USER_BRACKET",
            "PROJECT_TYPE_OLD",
            "CORPORATE_GROUP",
            "PRODUCT",
            "MONTHLY_SAAS_USD",
            "IMPLEMENTATION_FEE_USD",
            "PROJECT_DUE_DATE",
            "PREVIOUS_PIMS",
            "NUM_IMPLEMENTERS",
            "PROJECT_START_DATE",
            "CLINIC_TYPE",
            "SURVEY_CONTACT_FIRST_NAME",
            "SURVEY_CONTACT_LAST_NAME",
            "SURVEY_CONTACT_EMAIL",
            "SURVEY_CONTACT_PHONE",
            "'MAVENLINK'",
            'astatus."ͺAudit: Value"',
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
            "WORKSPACE_ID": "UNIQUE ID",
            "SAP_ID": "SAP ID",
            "PROJECT_TITLE": "Project Name",
            "TEAM_LEAD": "Team Lead / PM",
            "TEAM_LEAD_EMAIL": "Team Lead / PM Email",
            "TEAM_LEAD_REGION": "Implementer Office Base",
            "LEAD_IMPLEMENTER": "Lead implementer",
            "COUNTRY": "Region Country",
            "USER_BRACKET": "User Bracket",
            "PROJECT_TYPE_OLD": "Project Type",
            "CORPORATE_GROUP": "Group",
            "PRODUCT": "Product",
            "MONTHLY_SAAS_USD": "SaaS Fee",
            "IMPLEMENTATION_FEE_USD": "Implementation Fee",
            "PROJECT_DUE_DATE": "Project Go Live date",
            "PREVIOUS_PIMS": "Converted From",
            "NUM_IMPLEMENTERS": "# of Implementers",
            "PROJECT_START_DATE": "Project Start Date",
            "CLINIC_TYPE": "Hospital Type",
            "SURVEY_CONTACT_FIRST_NAME": "First Name",
            "SURVEY_CONTACT_LAST_NAME": "Last Name",
            "SURVEY_CONTACT_EMAIL": "Email",
            "SURVEY_CONTACT_PHONE": "Phone",
            "'MAVENLINK'": "Record Origin",
            'astatus."ͺAudit: Value"': "Audit Status",
        }

        return mapping

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        generated_date = datetime.now().date()
        # Z timezone means UTC+0
        currentutc_date = datetime.now(tz=pytz.timezone("UTC"))

        # check if there is value in from and to dates
        date_exclusion = "TRUE"

        if self.current_records():
            current_record_hook = f"DATEDIFF(day, TO_DATE(astatus.\"ͺAudit: Transaction Datetime\",'YYYY-MM-DDTHH:MI:SSZ'), '{currentutc_date}') = 0"
        else:
            current_record_hook = "TRUE"

        if self.custom_from == EMPTY_STRING_VAL or self.custom_to == EMPTY_STRING_VAL:
            date_exclusion = f"""
            (
                (
                astatus."ͺAudit: Value" = 'blue - Completed'
                AND
                    (
                        (
                        LOWER(PROJECT_TYPE_OLD) LIKE '%fresh%'
                        AND LOWER(PROJECT_TYPE_OLD) LIKE '%remote%'
                        ) OR 
                        (
                        LOWER(PROJECT_TYPE_OLD) = 'self implementation'
                        )
                    )
                AND {current_record_hook}
                )
                OR
                (
                    DATEDIFF(day, PROJECT_DUE_DATE, '{generated_date}') = {self.day_filter}
                    AND NOT LOWER(PROJECT_TYPE_OLD) LIKE '%fresh%'
                    AND NOT LOWER(PROJECT_TYPE_OLD) LIKE '%remote%'
                    AND LOWER(PROJECT_TYPE_OLD) NOT IN ('self implementation', 'other - no imp required')
                )
            )"""
        else:
            date_exclusion = f"""
            (
                (
                    PROJECT_DUE_DATE >= '{self.custom_from}' AND PROJECT_DUE_DATE <= '{self.custom_to}'
                ) OR (
                    ALT_SURVEY_DATE >= '{self.custom_from}' AND ALT_SURVEY_DATE <= '{self.custom_to}'
                ) OR (
                    TO_DATE(astatus.\"ͺAudit: Transaction Datetime\",'YYYY-MM-DDTHH:MI:SSZ') >= '{self.custom_from}'
                    AND
                    TO_DATE(astatus.\"ͺAudit: Transaction Datetime\",'YYYY-MM-DDTHH:MI:SSZ') <= '{self.custom_to}'
                )
            )
            """

        base_exclusions = [
            date_exclusion,
            "LOWER(PROJECT_STATUS) IN ('in progress', 'completed')",
            "ALT_SURVEY_DATE is NULL",
        ]

        # Check if there is a custom request against survey_date_logic
        if self.custom_from == EMPTY_STRING_VAL or self.custom_to == EMPTY_STRING_VAL:
            survey_date_logic = (
                f"DATEDIFF(day, ALT_SURVEY_DATE, '{generated_date}') = 0"
            )
            default_exclusion_logic = " AND ".join(base_exclusions)
            filter_logic = [f"(({default_exclusion_logic}) OR ({survey_date_logic}))"]
        else:
            filter_logic = base_exclusions

        return filter_logic + [
            "LOWER(PROJECT_TYPE_OLD) != 'other - no imp required'",
            "LOWER(PRODUCT) != 'test projects'",
            "ARCHIVED = FALSE",
            "(LOWER(FEEDBACK_CALL_COMPLETED) != 'waived' OR FEEDBACK_CALL_COMPLETED IS NULL)",
        ]

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

        return {
            "mavenlink_status_changes:astatus": f"""True:main.PROJECT_TITLE = astatus.\"ͺAudit: Record Project Name\"
            AND astatus."ͺAudit: Field" = 'status_key'
            AND astatus."ͺAudit: Previous Value" = 'green - In Progress'
            AND astatus."ͺAudit: Value" = 'blue - Completed'
            """
        }
