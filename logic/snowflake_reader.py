import pandas as pd
import sqlalchemy as sa

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from logging import Logger

from ezyvet.data.models.hook_model import HookModel
from ezyvet.data.models.voc_variables import GROUP_BY_COLUMN_EXCLUSIONS


class SnowflakeReader:
    """
    A class that grabs data from snowflake and return its dataframe equivalent
    """

    def __init__(self, logger: Logger):
        """
        Constructor for the SnowflakeReader class.

        Args:
        - logger (Logger): uses the logger for audit and debugging purposes

        Returns:
        - None
        """
        self.logger = logger

    def get_dataframe_from_snowflake(self, hook: HookModel) -> pd.DataFrame:
        """
        Returns the dataframe from snowflake

        Args:
        - hook (HookModel): holds the schema and connection id to be used for the creation of snowflake hook

        Returns:
        - DataFrame
        """
        try:
            table = hook.retrieve_table()
            cols = hook.retrieve_columns()
            renames = hook.retrieve_snowflake_csvmap()

        except Exception as e:
            self.logger.exception(
                f"Required definition not found. {hook.schema} {table} {datetime.now()}"
            )
            raise e

        # set up exclusions
        exclusion_list = hook.retrieve_exclusions()

        # start building the query:
        columns = []
        for c in cols:
            label = c.split(".", 1)
            # add quotes if column has string
            if " " in c:
                if len(label) > 1:
                    if '"' in label[(len(label) - 1)]:
                        column_name = label[(len(label) - 1)]
                    else:
                        column_name = f'"{label[(len(label) - 1)]}"'
                    escaped_col_name = f"{label[len(label)-2]}.{column_name}"
                else:
                    escaped_col_name = f'"{c}"'
            else:
                escaped_col_name = c

            new_label = renames.get(c, label[(len(label) - 1)]).strip('"')
            column = f'{escaped_col_name} AS "{new_label}"'
            columns.append(sa.text(column))

        stmt = sa.select(columns).select_from(sa.table(table).alias("main"))

        # add a join clause if the hook says so
        if hook.retrieve_join_tables():
            for key, val in hook.retrieve_join_tables().items():
                info = key.split(":", 1)
                outer_conditions = val.split(":", 1)
                stmt = stmt.join(
                    sa.table(info[0]).alias(info[1]),
                    sa.text(outer_conditions[1]),
                    isouter=bool(outer_conditions[0]),
                )

        # apply all the exclusions for the where clause
        for clause in exclusion_list:
            stmt = stmt.where(sa.text(clause))

        if hook.retrieve_group_by():
            grouping_list = [
                groupColumn
                for groupColumn in cols
                if not any(
                    excludes in groupColumn for excludes in GROUP_BY_COLUMN_EXCLUSIONS
                )
            ]
            stmt = stmt.group_by(sa.text(", ".join(grouping_list)))

        self.logger.info(str(stmt))

        alchemy_hook = SnowflakeHook(
            snowflake_conn_id=hook.retrieve_conn_id(),
            database=hook.retrieve_database(),
            schema=hook.retrieve_schema(),
        )
        alchemy_engine = alchemy_hook.get_sqlalchemy_engine()

        return pd.read_sql(stmt, alchemy_engine)
