from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from logging import Logger
from typing import List

from ezyvet.common.reporting import send_email
from ezyvet.data.models.voc_variables import (
    EMPTY_STRING_VAL,
    EMPTY_CG_VAL,
    EMPTY_REV_VAL,
    BUCKET,
    DEFAULT_PRODUCTION_NAME,
    INTEGRATION_ENVIRONMENT,
)


class IntegrationNotifier:
    """
    A class that handles nofication for the VoC Integration
    """

    def __init__(self, logger: Logger):
        """
        Constructor for the IntegrationNotifier class.

        Args:
        - logger (Logger): uses the logger for audit and debugging purposes

        Returns:
        - None
        """
        self.logger = logger
        date = datetime.now().date()

        # instantiate the necessary information
        self.mailing_dest = [
            "RolandoIsaac-Climaco@idexx.com",
            "rebecca-medeiros@idexx.com",
            "charlie-brink@idexx.com",
            "daniel-hooton@idexx.com",
            "miji-lee@idexx.com",
            "enrico-serrano@idexx.com",
            "Peter-Buckthought@idexx.com",
        ]

        self.issue_fail_mailing_dest = []
        self.mail_subj = f"VoC Integration - {date}"

        # how long presign will last
        # Changed the default expiry of the links to 7 days
        self.mail_expiry = 7

    def notify_holders(
        self,
        keys: List[str],
        custom_filename: str,
        skipped: bool = False,
        failed_list: bool = False,
    ) -> None:
        """
        Notify the holders. This will call the send functions

        Args:
        - keys (List[str]): keys of objects
        - custom_filename (str): Contains the custom filename if a new request exists
        - skipped (bool): Determines if we should send the notification for skipped pipeline. Default is False
        - failed_list (bool): Determines if the keys provided are for the file failure list. Default is False

        Returns:
        - None
        """

        if INTEGRATION_ENVIRONMENT != DEFAULT_PRODUCTION_NAME:
            self.logger.info(
                f"{INTEGRATION_ENVIRONMENT} is not {DEFAULT_PRODUCTION_NAME}. Exiting the send mail logic"
            )

            return None

        # adding more info logging for the keys
        self.logger.info(keys)
        self.logger.info(skipped)
        self.logger.info(failed_list)

        if skipped:
            self.send_skipped_mail_notification()
        else:
            self.send_mail_notification(keys, failed_list, custom_filename)

    def send_skipped_mail_notification(self) -> None:
        """
        Send mail to mailing list if the pipeline has been skipped.

        Returns:
        - None
        """

        # send mail to holders
        for email_destination in self.mailing_dest:
            send_email(
                email_destination,
                self.mail_subj,
                f"""
                No valid entries found for today.
            """,
            )

    def send_mail_notification(
        self, keys: List[str], failed_list: bool, custom_filename: str
    ) -> None:
        """
        Send mail

        Args:
        - keys (List[str]): keys of objects
        - failed_list (bool): returns if the provided keys are for the failed list.

        Returns:
        - None
        """
        if not keys:
            self.logger.error("No mails to send")
            exit(1)

        s3 = S3Hook("worker-user")
        message = ""

        if custom_filename != EMPTY_STRING_VAL:
            dates = custom_filename.split("_")
            requestdate = dates[0]
            fromdate = dates[1]
            todate = dates[2]
            message = f"""
                    Kindly refer to the links below containing records for the time range of {fromdate} to {todate} requested on {requestdate}:
"""

        # If the key isnt correct or if theres issues with signing, dont proceed with the rest of the logic.
        # Log the error and clear message.
        try:
            for key in keys:
                # check if the keys are empty and set the messages accordingly
                if key == EMPTY_CG_VAL:
                    message += """
                    Customer Gauge file is empty. Please refer to the failed email list
                    """
                    continue
                elif key == EMPTY_REV_VAL:
                    message += """
                    Revenue file is empty. Please refer to the failed email list
                    """
                    continue

                params = {"Bucket": BUCKET, "Key": key}
                expires_in = timedelta(days=self.mail_expiry)
                expires_on = datetime.now() + expires_in
                expires_in_s = int(expires_in.total_seconds())
                psu = s3.generate_presigned_url(
                    "get_object", params=params, expires_in=expires_in_s
                )
                if psu:
                    message += f"<li>{key} -> <a href='{psu}'>link</a> (expires on {expires_on})</li>"
        except Exception as e:
            self.logger.error(e)
            # clear out the message
            message = None
            exit(1)

        # prepare information for the email
        if failed_list:
            header_statement = (
                "CSV file download link for Voc Integration - Failed entries:"
            )
            mail_subj = f"{self.mail_subj} - Failed List"
            mail_dest = self.mailing_dest + self.issue_fail_mailing_dest
        else:
            header_statement = "CSV file download link for VoC Integration:"
            mail_subj = self.mail_subj
            mail_dest = self.mailing_dest

        # send mail to holders
        for email_destination in mail_dest:
            send_email(
                email_destination,
                mail_subj,
                f"""
                {header_statement}
                <ul>{"".join(message)}</ul>
            """,
            )

        # clear out the message
        message = None
