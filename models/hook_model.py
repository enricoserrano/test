from abc import ABC, abstractmethod
from typing import List

from ezyvet.data.models.voc_variables import EMPTY_STRING_VAL


class HookModel(ABC):
    """
    An abstract class that holds the data for snowflake connection
    """

    def __init__(self):
        self.day_filter = 21
        self.custom_from = EMPTY_STRING_VAL
        self.custom_to = EMPTY_STRING_VAL
        self.sap_ids = []

    def retrieve_conn_id(self) -> str:
        """
        Provides the connection id for the hook

        Returns:
        - str
        """

        return "idexx-vssanalytics-rw"

    def retrieve_group_by(self) -> bool:
        """
        Provides the tables to be joined for the query

        Returns:
        - bool
        """

        return False

    def current_records(self) -> bool:
        """
        Returns if we're grabbing the current records.
        This is for the previous failed records check

        Returns:
        - bool
        """

        return True

    @abstractmethod
    def retrieve_database(self) -> str:
        """
        Provides the database for the hook

        Returns:
        - str
        """
        pass

    @abstractmethod
    def retrieve_schema(self) -> str:
        """
        Provides the schema for the hook

        Returns:
        - str
        """
        pass

    @abstractmethod
    def retrieve_table(self) -> str:
        """
        Provides the table name for the hook

        Returns:
        - str
        """
        pass

    @abstractmethod
    def retrieve_columns(self) -> List[str]:
        """
        Provides the column for the hook

        Returns:
        - List[str]
        """
        pass

    @abstractmethod
    def retrieve_snowflake_csvmap(self) -> dict:
        """
        Provides the mapping for the snowflake and csv

        Syntax:
        SNOWFLAKE COLUMN : CSV COLUMN

        Returns:
        - dict
        """
        pass

    @abstractmethod
    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        pass

    @abstractmethod
    def retrieve_sap_id_column(self) -> str:
        """
        Provides the sap id column name for the query

        Returns:
        - str
        """
        pass

    @abstractmethod
    def retrieve_join_tables(self) -> dict:
        """
        Provides the tables to be joined for the query

        Syntax:
        {"table name:alias name": "left join bool:join conditions"}

        Returns:
        - dict
        """
        pass
