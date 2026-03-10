from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

import boto3

import time
import requests
import json
import glob
import os


settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)














@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    # TODO


    files_retrived = 0
    counter = 0
    suffix_url = "Z.json.gz"

    s3_client = boto3.client("s3")


    while files_retrived < file_limit:
        time.sleep(2)
        try:
            response = requests.get(f"{base_url}{counter:06d}{suffix_url}")

            if response.status_code == 200:
                
                data = response.json() 
                
                print(f"File {counter:06d} parsed successfully.")

               # Instead of saving locally, upload to S3
                s3_key = f"{s3_prefix_path}{counter:06d}.json"
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=json.dumps(data),
                    ContentType="application/json"
                )
                print(f"Uploaded to s3://{s3_bucket}/{s3_key}")
                
                files_retrived += 1
            else:
                print(f"File {counter:06d} not found (Status {response.status_code})")

        except Exception as e:
            print(f"Error processing {counter:06d}: {e}")
            break;

        counter += 5



    return "OK"








































@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO

    # 1. Create S3 client
    s3_client = boto3.client('s3')

    # 2. List all JSON files in S3
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"

    print(f"Listing files from s3://{s3_bucket}/{s3_prefix}")

    # Get list of objects
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=s3_prefix
    )

    # Check if any files exist
    if 'Contents' not in response:
        print("No files found in S3")
        return []

    files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
    files = sorted(files)

    print(f"Total files found: {len(files)}")

    # 3. Process each file from S3
    aircraft_list: list[dict] = []

    for file_key in files:
        try:
            # Download file from S3
            response = s3_client.get_object(
                Bucket=s3_bucket,
                Key=file_key
            )
            
            # Read and parse JSON
            file_content = response['Body'].read()
            data = json.loads(file_content)

            # Support both list-of-aircraft files and dicts with an 'aircraft' key
            candidates = []
            if isinstance(data, list):
                candidates = data
            elif isinstance(data, dict) and "aircraft" in data and isinstance(data["aircraft"], list):
                candidates = data["aircraft"]
            elif isinstance(data, dict):
                # Some files may be a single aircraft dict
                candidates = [data]

            for a in candidates:
                if not isinstance(a, dict):
                    continue
                icao = a.get("hex")
                if not icao:
                    # try common alternatives
                    icao = a.get("icao") or a.get("id")
                if not icao:
                    continue
                registration = a.get("r") or a.get("reg") or a.get("registration")
                ac_type = a.get("t") or a.get("type")
                aircraft_list.append({
                    "icao": icao, 
                    "registration": registration, 
                    "type": ac_type
                })
                
        except Exception as e:
            print(f"Error processing {file_key}: {e}")
            continue

    print(f"Total aircraft extracted: {len(aircraft_list)}")
    
    local_dir = "/home/marte/bts-bdp-assignment/data/raw/20231101"
    os.makedirs(local_dir, exist_ok=True)
    
    # Save as JSON file
    output_file = os.path.join(local_dir, "aircraft_list.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(aircraft_list, f, indent=2)
    
    print(f"Saved {len(aircraft_list)} aircraft to {output_file}")

    return "OK"