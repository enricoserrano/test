from typing import List

from ezyvet.data.models.hook_model import HookModel


class SuppSapHook(HookModel):
    """
    Class that connects to the snowflake SAP db
    """

    def retrieve_database(self) -> str:
        """
        Provides the database for the hook

        Returns:
        - str
        """

        return "VIEWS"

    def retrieve_schema(self) -> str:
        """
        Provides the schema for the hook

        Returns:
        - str
        """

        return "CUSTOMER_DATA"

    def retrieve_table(self) -> str:
        """
        Provides the table name for the hook

        Returns:
        - str
        """

        return "Customer"

    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """
        columns = [
            "SAP Customer ID Conversion",
            "SFDC Account Name",
            '"Region"',
            "Country Key",
            # "Region Desc", This is the full name of the Region
            # "Customer Group Region", This returns USA
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
            "SAP Customer ID Conversion": "SAP ID",
            "SFDC Account Name": "sap_account_name",
            '"Region"': "State",
            "Country Key": "Country",
        }

        return mapping

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        sap_ids = "'" + "','".join([str(x) for x in self.sap_ids if x]) + "'"
        # SAP Customer ID Conversion IS INT
        exclusions = [
            '"SAP Customer ID Conversion" IS NOT NULL',
            '"SAP Customer ID Conversion" != 0',
            '"SAP Customer ID Conversion" != 1',
            '"Marked For Deletion Flag" is NULL',
            f'"SAP Customer ID Conversion" in ({sap_ids})',
        ]

        return exclusions

    def retrieve_sap_id_column(self) -> str:
        """
        Provides the sap id column name for the query

        Returns:
        - str
        """

        return '"SAP Customer ID Conversion"'

    def retrieve_join_tables(self) -> dict:
        """
        Provides the tables to be joined for the query

        Syntax:
        {"table name:alias name": "left join bool:join conditions"}

        Returns:
        - dict
        """

        return {}
