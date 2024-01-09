from typing import List

from ezyvet.data.hooks.main_maven_hook import MainMavenHook


class MainMavenPrevHook(MainMavenHook):
    """
    Class that connects to the snowflake mavenlink db.
    This will retrieve records for prior failed entries
    """

    def retrieve_exclusions(self) -> List[str]:
        """
        Provides the exclusions for the query

        Returns:
        - List[str]
        """
        sap_ids = "'" + "','".join([str(x) for x in self.sap_ids if x]) + "'"
        exclusions = [
            f"SAP_ID in ({sap_ids})",
            "PROJECT_STATUS IN ('In Progress', 'Completed')",
            "PRODUCT != 'Test Projects'",
            "ARCHIVED = FALSE",
            "(FEEDBACK_CALL_COMPLETED != 'Waived' OR FEEDBACK_CALL_COMPLETED IS NULL)",
            "PROJECT_TYPE_OLD != 'OTHER - No imp required'",
        ]

        return exclusions

    def current_records(self) -> bool:
        """
        Returns if we're grabbing the current records.
        This is for the previous failed records check

        Returns:
        - bool
        """

        return False
