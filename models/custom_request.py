class customrequest:
    """
    Contains the variables related to custom requests.
    """

    FOLDER = "CUSTOM"
    """
    Folder path where the custom requested files will be uploaded to
    """

    DATE_RANGE = 5
    """
    Range where we will still check if the request has been accomplished
    """

    REQ_LIST = {
        "2023-12-14": "2023-10-31_2023-11-20",  # DT-3080
        "2023-11-17": "2023-01-01_2023-10-31",
        "2023-11-16": "2023-10-01_2023-10-31",  # test file
        "2023-11-13": "2023-10-01_2023-10-31",  # test file
    }
    """
    Contains the list of requests. If the requested file is not available in s3, it will be generated rather than running the default pipeline.
    
    ALWAYS put the latest request at the TOP of the dictionary

    Syntax:
    {
        "Date Requested": "DateFrom_DateTo",

        "YYYY-MM-DD":  "YYYY-MM-DD_YYYY-MM-DD"
    }
    """
