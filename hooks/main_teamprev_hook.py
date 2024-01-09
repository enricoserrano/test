from typing import List

from ezyvet.data.hooks.main_team_hook import MainTeamHook


class MainTeamPrevHook(MainTeamHook):
    """
    Class that connects to the snowflake teamwork db.
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
            f"main.SAP_ID in ({sap_ids})",
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
