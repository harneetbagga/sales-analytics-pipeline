import os
import time

import pytest

# This file is a LIVE check, not a unit test: importing it starts a real QuickSight SPICE
# refresh. The datasets use Direct Query now, so it is manual-only. To run it:
#   RUN_QUICKSIGHT_REFRESH_TEST=1 QUICKSIGHT_ACCOUNT_ID=<aws account id> pytest dags/tests/test_ingestion.py -s
if not (os.environ.get("RUN_QUICKSIGHT_REFRESH_TEST") and os.environ.get("QUICKSIGHT_ACCOUNT_ID")):
    pytest.skip("manual QuickSight refresh check (see the comment at the top of this file)",
                allow_module_level=True)

import boto3  # noqa: E402  (after the skip, so collecting this file needs no AWS libraries)

QUICKSIGHT_ACCOUNT_ID = os.environ["QUICKSIGHT_ACCOUNT_ID"]  # your AWS account id
QUICKSIGHT_REGION = "us-east-1"

# Test against just one dataset first
DATASET_ID = "6ab7081a-50b9-4426-92bf-6c99bdada985"  # Outbound Setter Report

client = boto3.client("quicksight", region_name=QUICKSIGHT_REGION)

ingestion_id = f"testrefresh{int(time.time())}"

resp = client.create_ingestion(
    DataSetId=DATASET_ID,
    IngestionId=ingestion_id,
    AwsAccountId=QUICKSIGHT_ACCOUNT_ID,
    IngestionType="FULL_REFRESH",
)
print("Create response:", resp["IngestionStatus"])
print("Ingestion ID:", ingestion_id)

# Poll for completion
for _ in range(20):
    status_resp = client.describe_ingestion(
        DataSetId=DATASET_ID,
        IngestionId=ingestion_id,
        AwsAccountId=QUICKSIGHT_ACCOUNT_ID,
    )
    ingestion = status_resp["Ingestion"]
    status = ingestion["IngestionStatus"]
    print("Status:", status)

    if status in ("COMPLETED", "FAILED", "CANCELLED"):
        if status == "FAILED":
            print("Error info:", ingestion.get("ErrorInfo"))
        else:
            print("Rows ingested:", ingestion.get("RowInfo"))
        break

    time.sleep(5)