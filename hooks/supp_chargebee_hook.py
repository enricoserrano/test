from typing import List

from ezyvet.data.models.hook_model import HookModel


class SuppChargeBeeHook(HookModel):
    """
    Class that connects to the snowflake neo charges db
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

        return "CHARGEBEE"

    def retrieve_table(self) -> str:
        """
        Provides the table name for the hook

        Returns:
        - str
        """

        return "cdl_chargebee"

    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """
        columns = [
            "SAP",
            "USERCOUNT",
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
            "SAP": "SAP ID",
            "USERCOUNT": "Team UserCount",
        }

        return mapping

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        sap_ids = "'" + "','".join([str(x) for x in self.sap_ids if x]) + "'"
        ## SAP IS STRING
        exclusions = [
            "STATUS in ('active', 'future')",
            "SAP IS NOT NULL",
            "SAP != '0'",
            "SAP != '1'",
            "SAP != ''",
            f"SAP in ({sap_ids})",
        ]

        return exclusions

    def retrieve_sap_id_column(self) -> str:
        """
        Provides the sap id column name for the query

        Returns:
        - str
        """

        return "SAP"

    def retrieve_join_tables(self) -> dict:
        """
        Provides the tables to be joined for the query

        Syntax:
        {"table name:alias name": "left join bool:join conditions"}

        Returns:
        - dict
        """

        return {}
