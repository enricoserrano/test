from typing import List

from ezyvet.data.models.hook_model import HookModel


class SuppDxHook(HookModel):
    """
    Class that connects to the snowflake DX db
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

        return "L12_DX_REVENUE"

    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """
        columns = ["SHIP_SAP_NUMBER_CONVERSION", "L12_CAG_RECURRING_REVENUE"]

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
            "SHIP_SAP_NUMBER_CONVERSION": "SAP ID",
            "L12_CAG_RECURRING_REVENUE": "IDEXX DX Spend",
        }

        return mapping

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        sap_ids = "'" + "','".join([str(x) for x in self.sap_ids if x]) + "'"

        # SHIP_SAP_NUMBER_CONVERSION IS INT
        exclusions = [
            "SHIP_SAP_NUMBER_CONVERSION IS NOT NULL",
            "SHIP_SAP_NUMBER_CONVERSION != 0",
            "SHIP_SAP_NUMBER_CONVERSION != 1",
            f'"SHIP_SAP_NUMBER_CONVERSION" in ({sap_ids})',
        ]

        return exclusions

    def retrieve_sap_id_column(self) -> str:
        """
        Provides the sap id column name for the query

        Returns:
        - str
        """

        return '"SHIP_SAP_NUMBER_CONVERSION"'

    def retrieve_join_tables(self) -> dict:
        """
        Provides the tables to be joined for the query

        Syntax:
        {"table name:alias name": "left join bool:join conditions"}

        Returns:
        - dict
        """

        return {}
