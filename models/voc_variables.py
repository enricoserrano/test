"""
Contains the variables for the VOC integration
"""

# Integration Variables

INTEGRATION_TAGS = ["data", "datateam@ezyvet.com"]
"""
Tags used to determine tasks and dags generated in airflow
"""

MAX_ACTIVE_RUN = 1
"""
Number of concurrent runs allowed
"""

TIMEZONE = "US/Central"
"""
Timezone for the DAG
"""

CRON_SCHEDULE = "0 8 * * *"
"""
CRON Schedule for automated task executions
"""

DEFAULT_PRODUCTION_NAME = "prod"
"""
Default Production name
"""

INTEGRATION_ENVIRONMENT = DEFAULT_PRODUCTION_NAME
"""
Environment of the integration
"""

# Query Variables

GROUP_BY_COLUMN_EXCLUSIONS = ["SUM("]
"""
Dictates the columns that will be excluded in the group by
"""

# DataFrame Merge Variables
VOC_JOIN_COLUMN = "SAP ID"
"""
Dictates the column name of where the data frames will merge against each other
"""

# DataFrame Merge Variables
VOC_JOIN_UNIQUE_ID = "UNIQUE ID"
"""
Unique identifier used to link failed records for reprocessing
"""

# Compute Fields Variables
TOUCHPOINT_DEFAULT_VALUE = "Implementation"
"""
Default value for touchpoint column
"""

ROLE_DEFAULT_VALUE = "Project Stakeholder"
"""
Default value for role column
"""

# Column Fixer Variables
DATE_COLUMNS = ["Project Go Live date", "Project Start Date"]
"""
Columns that should be casted as the type date
"""

STRING_COLUMNS = ["User Bracket", "# of Implementers"]
"""
Columns that should be casted as the type string
"""

FLOAT_COLUMNS = ["SaaS Fee", "Implementation Fee", "IDEXX DX Spend"]
"""
Columns that should be casted as the type float
"""

# COMPARISON COLUMNS TO FILTER NOW SUCCESSFUL RECORDS
OLD_COMPARISON_COLUMNS = ["UNIQUE ID", "Role", "Project Name", "Project Type"]
"""
Columns to be used to determine if the record is the previous failed record
"""

# COMPARISON COLUMNS TO CHECK IF DATA IS DUPLICATED
DUPLICATED_RAW_COLUMNS = [
    "UNIQUE ID",
    "Project Name",
    "Project Type",
    "Project Go Live date",
    "Product",
]
"""
Columns to be used to determine if the raw data is duplicated
"""

DUPLICATED_CONSOLIDATED_COLUMNS = [
    "SAP ID",
    "Project Name",
    "Project Type",
    "Project Go Live date",
    "Product",
    "Role",
]
"""
Columns to be used to determine if the consolidated data is duplicated
"""

# Upload Path Variables
BUCKET = "ezyvetconversions"
"""
S3 Bucket Name
"""

DEFAULT_FILE_PATH = "integrations"
"""
S3 Integrations Folder
"""

INTEGRATION_NAME = "voc"
"""
S3 Project Folder
"""

CONSOLIDATED_FOLDER_NAME = "consolidated"
"""
S3 Consolidated Folder Name
"""

# Filenames
FILENAME_CG = "CGImport"
"""
Filename prefix to be used for the CSVs generated for Customer Gauge
"""

FILENAME_REV = "CGRevenue"
"""
Filename prefix to be used for the CSVs generated for revenue
"""

FILENAME_FAILED = "CGFailedRecords"
"""
Filename to be used for the records that has incomplete data
"""

# Empty Variables Value
EMPTY_STRING_VAL = "NaN"
"""
Default value for empty string
"""

EMPTY_CG_VAL = "CG_EMPTY"
"""
Default Value for Customer Gauge key if it is empty
"""

EMPTY_REV_VAL = "REV_EMPTY"
"""
Default Value for revenue key if it is empty
"""

# Frame Generator Variables
VOC_REVENUE_FRAME_LIST = [
    "Account Name - SAP ID",
    "Project Go Live date",
]
"""
Columns to be copied from processed dataframe to be used for revenue CSV
"""

# Mapping the Voc Revenue CSV Requirements
VOC_REVENUE_EXPORT = [
    "Account Name - SAP ID",
    "Revenue Start Date",
    "Revenue End Date",
    "Revenue Amount",
    "Revenue Type",
    "Description",
]
"""
Columns that will be shown in the generated revenue CSV
"""

# Mapping the additional columns for the survey export failed entries
VOC_SURVEY_FAILED_EXPORT_ADDONS = [
    "Record Origin",
    "UNIQUE ID",
]
"""
Additional columns that will be shown in the generated failed revenue on top of the default Customer Gauge columns
"""

# Mapping the Voc Survery CSV Requirements
VOC_SURVEY_EXPORT = [
    "First Name",
    "Last Name",
    "Email",
    "Account Name - SAP ID",
    "Team Lead / PM",
    "Team Lead / PM Email",
    "Touchpoint",
    "SAP ID",
    "Phone",
    "Country",
    "Locale",
    "Job Level",
    "Region",
    "Customer Tier",
    "Product",
    "Project Name",
    "Project Type",
    "User Bracket",
    "SaaS Fee",
    "Implementation Fee",
    "Project Go Live date",
    "IDEXX DX Spend",
    "Loyalty",
    "Converted From",
    "Lead implementer",
    "Implementer Office Base",
    "# of Implementers",
    "Project Start Date",
    "Hospital Type",
    "Group",
    "ESAM",
    "State",
    "Role",
]
"""
Columns that will be shown in the generated Customer Gauge CSV
"""

# NA Values for Pandas
VOC_NA_VALUES = [
    "",
    "#N/A",
    "#N/A N/A",
    "#NA",
    "-1.#IND",
    "-1.#QNAN",
    "-NaN",
    "-nan",
    "1.#IND",
    "1.#QNAN",
    "<NA>",
    "N/A",
    #   "NA",
    "NULL",
    "NaN",
    "n/a",
    "nan",
    "null",
]
