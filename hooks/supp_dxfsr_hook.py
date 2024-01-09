from typing import List

from ezyvet.data.models.hook_model import HookModel


class SuppDXFSRHook(HookModel):
    """
    Class that connects to the snowflake DXFSR db
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

        return "LIST_MANAGEMENT"

    def retrieve_table(self) -> str:
        """
        Provides the table name for the hook

        Returns:
        - str
        """

        return "VDC_FSR_ACCOUNT_CONTACT"

    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """
        columns = [
            "SAP_ID",
            "IDEXX_REP_FIRST_NAME",
            "IDEXX_REP_LAST_NAME",
            "IDEXX_REP_EMAIL",
            "IDEXX_REP_PHONEW",
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
            "SAP_ID": "SAP ID",
            "IDEXX_REP_FIRST_NAME": "DXFSR_First_Name",
            "IDEXX_REP_LAST_NAME": "DXFSR_Last_Name",
            "IDEXX_REP_EMAIL": "DXFSR_Email",
            "IDEXX_REP_PHONEW": "DXFSR_Phone",
        }

        return mapping

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        sap_ids = "'" + "','".join([str(x) for x in self.sap_ids if x]) + "'"
        # SAP_ID IS STRING
        exclusions = [
            "SAP_ID IS NOT NULL",
            "SAP_ID != '0'",
            "SAP_ID != '1'",
            "SAP_ID != ''",
            f"SAP_ID in ({sap_ids})",
            "ROLE_IN_TERRITORY = 'DX FSR'",
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

        return {}
