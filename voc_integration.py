import logging
import pytz

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pandas import DataFrame
from typing import Optional, List

from ezyvet.data.models.voc_variables import (
    CRON_SCHEDULE,
    EMPTY_STRING_VAL,
    EMPTY_CG_VAL,
    EMPTY_REV_VAL,
    FILENAME_REV,
    FILENAME_CG,
    FILENAME_FAILED,
    INTEGRATION_TAGS,
    MAX_ACTIVE_RUN,
    TIMEZONE,
)

log = logging.getLogger(__name__)


@dag(
    schedule=CRON_SCHEDULE,
    start_date=datetime(2023, 9, 1, tzinfo=pytz.timezone(TIMEZONE)),
    catchup=False,
    tags=INTEGRATION_TAGS,
    max_active_runs=MAX_ACTIVE_RUN,
)
def voc_integration():
    @task
    def return_open_custom_filename() -> str:
        """
        Returns if the items in the custom request file has been generated already

        Returns:
        - str
        """
        import re
        from s3fs import S3FileSystem
        from ezyvet.data.logic.file_handler import FileHandler
        from ezyvet.data.models.custom_request import customrequest
        from ezyvet.data.models.voc_variables import (
            BUCKET,
            DEFAULT_FILE_PATH,
            DEFAULT_PRODUCTION_NAME,
            INTEGRATION_NAME,
            INTEGRATION_ENVIRONMENT,
        )

        current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        recent_requested_files: List[str] = []

        log.info("Current Request List:")
        log.info(customrequest.REQ_LIST)

        for reqdate, reqrange in customrequest.REQ_LIST.items():
            reqdate_timestamp = datetime.strptime(reqdate, "%Y-%m-%d")
            days_passed = abs((current_date - reqdate_timestamp).days)

            # we only check entries less than N days
            if days_passed > customrequest.DATE_RANGE:
                continue

            recent_requested_files.append(f"{reqdate}_{reqrange}")

        log.info("Recent Requested Files List:")
        log.info(recent_requested_files)

        if not recent_requested_files:
            return EMPTY_STRING_VAL

        fh = FileHandler(log)

        path = f"s3://{BUCKET}/{DEFAULT_FILE_PATH}/{INTEGRATION_NAME}/{customrequest.FOLDER}"
        files = []
        i = 0

        try:
            while i <= (customrequest.DATE_RANGE + 1):
                date_finder = (datetime.now() - timedelta(days=i)).date()
                files += S3FileSystem().find(
                    fh.dynamic_folderpath_generator(date_finder, path)
                )
                i += 1
        except Exception as e:
            log.info("Failed to connect to s3. Skipping step")
            log.info(e)
            return EMPTY_STRING_VAL

        recent_filenames_list: List[str] = []
        for filepath in files:
            temp_filename = filepath.split("/")[len(filepath.split("/")) - 1]

            if temp_filename == "":
                continue
            recent_filenames_list.append(
                re.sub("[aA-zZ]{1,}_", "", temp_filename.split(".")[0])
            )

        log.info("Recent Uploaded Files List:")
        log.info(recent_filenames_list)

        req_to_accomplish = list(
            sorted(
                set(recent_requested_files) - set(recent_filenames_list),
                key=recent_requested_files.index,
            )
        )

        log.info("Requests Left to Accomplish:")
        log.info(req_to_accomplish)

        if not req_to_accomplish:
            return EMPTY_STRING_VAL

        # we only do one of the requests
        return req_to_accomplish[0]

    @task
    def read_previous_failed_records_from_s3(
        request_filename: str = EMPTY_STRING_VAL,
    ) -> DataFrame:
        """
        Read data from S3 and if successful return the dataframe

        Args:
        - custom_request (bool): Checks if the execution is a custom request

        Returns:
        - DataFrame
        """
        from ezyvet.data.logic.file_handler import FileHandler
        from ezyvet.data.models.voc_variables import (
            BUCKET,
            DEFAULT_FILE_PATH,
            INTEGRATION_NAME,
        )

        # exit early if the request filename is valid
        if request_filename != EMPTY_STRING_VAL:
            return DataFrame()

        fh = FileHandler(log)
        previous_date = (datetime.now() - timedelta(days=1)).date()
        dynamic_filename = fh.dynamic_filename_generator(FILENAME_FAILED, previous_date)
        dynamic_path = fh.dynamic_folderpath_generator(previous_date)
        key = (
            f"{DEFAULT_FILE_PATH}/{INTEGRATION_NAME}/{dynamic_path}/{dynamic_filename}"
        )
        path = f"s3://{BUCKET}/{key}"
        df = fh.read_csvfile_to_frame(path)

        log.info(df.to_json())

        return df

    @task
    def read_snowflake_to_object(
        previous_failed_frame=DataFrame(), custom_filename: str = EMPTY_STRING_VAL
    ) -> Optional[DataFrame]:
        """
        Read data from snowflake and generate data frames based on return values

        Args:
        - previous_failed_frame (DataFrame): contains the records for the previous failed frame
        - custom_file (str): contains the custom file name, returns a string NaN if empty

        Returns:
        - Dataframe
        """
        from ezyvet.data.logic.frame_generator import FrameGenerator
        from ezyvet.data.logic.frame_holder import FrameHolder
        from ezyvet.data.models.voc_variables import VOC_JOIN_COLUMN

        holder = FrameHolder(log)
        holder.retrieve_base_frame(previous_failed_frame, custom_filename)
        ids = holder.base_frame[VOC_JOIN_COLUMN].to_list()

        # check if theres any sap ids that were returned. if not we just escape the whole function
        if not ids:
            return None

        # create instance of the frame generator
        fg = FrameGenerator(log)

        # retrieve the supplemental frames from snowflake
        holder.retrieve_supplemental_frames(ids)

        # merge the frames via frame generator
        combined_data_frame = fg.merge_frames(
            holder.base_frame, holder.supplemental_frames
        )

        # remove the hook values from memory
        holder = None

        log.info(combined_data_frame.to_json())
        return combined_data_frame

    @task
    def duplicate_sanity_check(df: DataFrame) -> DataFrame:
        """
        There are instances where the duplicate check in read snowflake to object
        doesnt execute properly. We will rerun the method here before continuing the pipeline.

        It should just skip if there are no more duplicates.

        Args:
        - df (DataFrame): DataFrame to be cleaned

        Returns:
        - DataFrame
        """
        from ezyvet.data.logic.frame_holder import FrameHolder

        fh = FrameHolder(log)

        checked_frame = fh.remove_duplicates_from_frames(df)

        return checked_frame

    @task
    def process_computed_data(df: DataFrame) -> DataFrame:
        """
        Process the dataframe to acquire computed values

        Args:
        - df (DataFrame): Dataframe that will be transformed

        Returns:
        - Dataframe
        """
        from ezyvet.data.logic.compute_fields import ComputeFields

        cf = ComputeFields(log)
        processed_frame = cf.add_computed_fields(df)

        return processed_frame

    @task
    def create_revenue_frame(df: DataFrame) -> DataFrame:
        """
        Creates revenue frame based from the existing one

        Args:
        - df (DataFrame): Dataframe where we will copy from

        Returns:
        - Dataframe
        """
        from ezyvet.data.logic.frame_generator import FrameGenerator

        fg = FrameGenerator(log)
        revenue_frame = fg.create_revenue_frame(df)

        return revenue_frame

    @task
    def create_cg_frame(df: DataFrame) -> DataFrame:
        """
        Creates cg frame based from the existing one

        Args:
        - df (DataFrame): Dataframe where we will copy from

        Returns:
        - Dataframe
        """
        from ezyvet.data.logic.frame_generator import FrameGenerator

        fg = FrameGenerator(log)
        cg_frame = fg.create_processed_frame(df)

        return cg_frame

    @task
    def separate_valid_invalid_entries(
        df: DataFrame,
        valid: bool,
        previousFailedRecords: DataFrame = DataFrame(),
        custom_filename: str = EMPTY_STRING_VAL,
    ) -> DataFrame:
        """
        Separates the valid from the invalid entries

        Args:
        - df (DataFrame): Dataframe where we will filter out the values
        - valid (bool): Determines whether we are getting valid or invalid values
        - previousFailedRecords (DataFrame): Dataframe that contains the previous failed records
        - custom_filename (str): Contains the customfile name for requests. Returns NaN if empty

        Returns:
        - DataFrame
        """
        from ezyvet.data.logic.frame_generator import FrameGenerator

        fg = FrameGenerator(log)
        if valid:
            # We should only use the previous frame if the custom_filename is empty
            if custom_filename != EMPTY_STRING_VAL:
                previousFrame = DataFrame()
            else:
                previousFrame = previousFailedRecords
            result_frame = fg.separate_valid_frames(df, previousFrame)
        else:
            result_frame = fg.separate_invalid_frames(df)

        return result_frame

    @task
    def push_data_to_s3_bucket(
        df: DataFrame, filename: str, custom_filename: str = EMPTY_STRING_VAL
    ) -> Optional[str]:
        """
        Generate CSV from Data frame and Upload it to S3

        Args:
        - df (DataFrame): Dataframe to be converted to csv
        - filename (str): csv filename
        - custom_filename (str):
        - timestamp (float): return

        Returns:
        - str
        """
        from ezyvet.data.logic.file_handler import FileHandler
        from ezyvet.data.models.custom_request import customrequest
        from ezyvet.data.models.voc_variables import (
            BUCKET,
            DEFAULT_FILE_PATH,
            INTEGRATION_NAME,
        )

        if df.empty:
            log.info("Returning empty key")
            return EMPTY_STRING_VAL

        fh = FileHandler(log)

        # build the filename
        current_date = datetime.now().date()
        if custom_filename == EMPTY_STRING_VAL:
            dynamic_filename = fh.dynamic_filename_generator(filename, current_date)
            dynamic_path = fh.dynamic_folderpath_generator(current_date)
        else:
            dynamic_filename = f"{filename}_{custom_filename}.csv"
            dynamic_path = fh.dynamic_folderpath_generator(
                current_date, customrequest.FOLDER
            )

        key = (
            f"{DEFAULT_FILE_PATH}/{INTEGRATION_NAME}/{dynamic_path}/{dynamic_filename}"
        )
        path = f"s3://{BUCKET}/{key}"
        fh.upload_frame_to_csv(path, df)

        log.info(key)
        return key

    @task
    def send_mail(
        custom_filename: str = EMPTY_STRING_VAL,
        custom_keys: List[str] = [EMPTY_STRING_VAL, EMPTY_STRING_VAL],
        consolidated_keys: List[str] = [EMPTY_STRING_VAL, EMPTY_STRING_VAL],
        failed_key: str = EMPTY_STRING_VAL,
    ) -> None:
        """
        Send mail

        Args:
        - custom_filename (str): contains the custom file name if there is a new requested file
        - custom_keys (List[str]): contains the keys for the recently uploaded csv
        - consolidated_keys (List[str]): contains the keys for consolidated csv
        - failed_key (str): contains the key for the failed records csv

        Returns:
        - None
        """
        from ezyvet.data.logic.notifier import IntegrationNotifier

        if custom_filename != EMPTY_STRING_VAL:
            key_list = custom_keys
        else:
            key_list = consolidated_keys

        iNotify = IntegrationNotifier(log)

        # capture empty keys
        if key_list[0] != EMPTY_STRING_VAL:
            final_cg_key = key_list[0]
        else:
            final_cg_key = EMPTY_CG_VAL

        if key_list[1] != EMPTY_STRING_VAL:
            final_rev_key = key_list[1]
        else:
            final_rev_key = EMPTY_REV_VAL

        iNotify.notify_holders([final_cg_key, final_rev_key], custom_filename)

        # send the failed list email as well
        if failed_key != EMPTY_STRING_VAL:
            iNotify.notify_holders([failed_key], custom_filename, False, True)

    @task
    def consolidate_cg_records(
        is_import: bool = True, custom_filename: str = EMPTY_STRING_VAL
    ) -> str:
        """
        Consolidates the records for previous days to a single file and upload it to s3

        Args:
        - is_import (bool): Flag whether to consolidate cgimport or revenue file
        - custom_filename (str): used to determine whether we should skip the task

        Returns:
        - str
        """
        from ezyvet.data.logic.file_handler import FileHandler
        from ezyvet.data.models.voc_variables import (
            BUCKET,
            CONSOLIDATED_FOLDER_NAME,
            DEFAULT_FILE_PATH,
            DUPLICATED_CONSOLIDATED_COLUMNS,
            INTEGRATION_NAME,
            VOC_REVENUE_EXPORT,
        )

        # sanity check to skip task if we have a custom filename
        if custom_filename != EMPTY_STRING_VAL:
            return EMPTY_STRING_VAL

        current_date = datetime.now()
        current_day = current_date.weekday()

        # 1 is tuesday and 3 is thursday
        if current_day != 1 and current_day != 3:
            log.info("Won't consolidate records, date is neither tuesday nor thursday")
            log.info(current_day)
            log.info(current_date)
            return EMPTY_STRING_VAL

        fh = FileHandler(log)

        if is_import:
            prefix = FILENAME_CG
            column_duplicate_filter = DUPLICATED_CONSOLIDATED_COLUMNS
        else:
            prefix = FILENAME_REV
            column_duplicate_filter = VOC_REVENUE_EXPORT

        finalised_frame = fh.consolidate_dataframe(
            current_day, column_duplicate_filter, prefix, [], current_date
        )

        # upload the dataframe
        upload_folder = fh.dynamic_folderpath_generator(
            current_date.date(), CONSOLIDATED_FOLDER_NAME
        )
        upload_filename = fh.dynamic_filename_generator(prefix, current_date.date())
        upload_key = (
            f"{DEFAULT_FILE_PATH}/{INTEGRATION_NAME}/{upload_folder}/{upload_filename}"
        )
        upload_path = f"s3://{BUCKET}/{upload_key}"
        fh.upload_frame_to_csv(upload_path, finalised_frame)

        return upload_key

    @task.branch(task_id="check_skipped_mail")
    def check_skipped_mail(Value) -> str:
        """
        Checks if the value is not empty.
        Stops the pipeline if it fails the check

        Args:
        - Value (object): Object to check if it is not empty

        Returns:
        - str
        """
        if Value is not None:
            return "duplicate_sanity_check"
        else:
            return "send_skipped_mail"

    @task.branch(task_id="check_send_mail")
    def check_send_mail(custom_filename: str, consolidated_keys: List[str]) -> str:
        """
        Checks if the value is not empty.
        Stops the pipeline if it fails the check

        Args:
        - Value (object): Object to check if it is not empty

        Returns:
        - str
        """
        if (
            custom_filename == EMPTY_STRING_VAL
            and consolidated_keys[0] == EMPTY_STRING_VAL
            and consolidated_keys[1] == EMPTY_STRING_VAL
        ):
            return None
        else:
            return "send_mail"

    @task
    def send_skipped_mail() -> None:
        """
        Sends the skipped pipeline mail

        Returns:
        - None
        """
        from ezyvet.data.logic.notifier import IntegrationNotifier

        iNotify = IntegrationNotifier(log)
        iNotify.notify_holders([], EMPTY_STRING_VAL, True)

    # check if all custom files have been created
    custom_filename = return_open_custom_filename()

    # load the previous failed frame is possible
    previous_failed_frame = read_previous_failed_records_from_s3(custom_filename)

    # create and populate the base frame
    raw_dataframe = read_snowflake_to_object(previous_failed_frame, custom_filename)

    branch_op = check_skipped_mail(raw_dataframe)

    # send skipped mail if possible
    skipped = send_skipped_mail()

    # sanity check the duplicates if they have been removed in the list
    checked_dataframe = duplicate_sanity_check(raw_dataframe)

    # logic when we can process the data
    base_frame = process_computed_data(checked_dataframe)

    # create the frames to be uploaded base from the frame generated previously
    revenue_dataframe = create_revenue_frame(base_frame)
    processed_dataframe = create_cg_frame(base_frame)

    # separate the processed frame between valid and invalid entries
    valid_frame = separate_valid_invalid_entries(
        processed_dataframe, True, previous_failed_frame, custom_filename
    )
    invalid_frame = separate_valid_invalid_entries(processed_dataframe, False)

    # push the frames to s3
    rev_key = push_data_to_s3_bucket(revenue_dataframe, FILENAME_REV, custom_filename)
    cg_key = push_data_to_s3_bucket(valid_frame, FILENAME_CG, custom_filename)
    failed_key = push_data_to_s3_bucket(invalid_frame, FILENAME_FAILED, custom_filename)

    consolidated_cgimport_records = consolidate_cg_records(True, custom_filename)
    consolidated_cgrevenue_records = consolidate_cg_records(False, custom_filename)

    branch_send_op = check_send_mail(
        custom_filename, [consolidated_cgimport_records, consolidated_cgrevenue_records]
    )

    notif_mail = send_mail(
        custom_filename,
        [cg_key, rev_key],
        [consolidated_cgimport_records, consolidated_cgrevenue_records],
        failed_key,
    )

    # setup dependencies
    raw_dataframe >> branch_op >> checked_dataframe
    branch_op >> skipped
    cg_key >> consolidated_cgimport_records
    rev_key >> consolidated_cgrevenue_records
    branch_send_op >> notif_mail


voc_integration()
