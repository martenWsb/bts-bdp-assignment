import os
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

import time
import requests
import json
import glob

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)



















@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    # TODO Implement download

    # Urls
    folder_name = os.path.join(settings.raw_dir, "20231101")
    os.makedirs(folder_name, exist_ok=True)
    base_url = settings.source_url + "/2023/11/01/"
    suffix_url = "Z.json.gz"
    print(folder_name)


    # Vars for the while loop
    files_retrived = 0
    counter = 0

    while files_retrived < file_limit:
        time.sleep(2)
        try:
            response = requests.get(f"{base_url}{counter:06d}{suffix_url}")

            if response.status_code == 200:
                # 1. Use the built-in .json() method. 
                # It handles decryption/decompression automatically.
                data = response.json() 
                
                print(f"File {counter:06d} parsed successfully.")

                # Save the dictionary to a local file
                file_path = os.path.join(folder_name, f"{counter:06d}.json")
                with open(file_path, 'w', encoding='utf-8') as out_file:
                    json.dump(data, out_file, indent=4)
                
                files_retrived += 1
            else:
                print(f"File {counter:06d} not found (Status {response.status_code})")

        except Exception as e:
            print(f"Error processing {counter:06d}: {e}")
            break;

        counter += 5






    return "OK"

























@s1.post("/aircraft/prepare")
def prepare_data() -> list[dict]:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO

    # 1. Read the files (all JSON files in the folder)
    folder_name = os.path.join(settings.raw_dir, "20231101")
    files = sorted(glob.glob(os.path.join(folder_name, "*.json")))

    # 2. Print the results, read contents and extract aircraft info
    print(f"Total files found: {len(files)}")
    aircraft_list: list[dict] = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)

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
                aircraft_list.append({"icao": icao, "registration": registration, "type": ac_type})

        except Exception as e:
            print(f"Failed reading {f}: {e}")

    print(f"Total aircraft extracted: {len(aircraft_list)}")




    return aircraft_list[:30]






























@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}]












































@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}