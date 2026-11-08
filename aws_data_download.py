import os
import zipfile

import requests
from utils.logger_config import setup_logger

# from .utils.logger_config import setup_logger

logger = setup_logger("AWS-data-pipeline", "logs")


def get_file_map(uris):
    logger.info("Mapping the file names with corresponding uris")
    file_map = {}
    for x in uris:
        filename = x.split("/")[-1]
        file_map[filename] = x
    logger.info("Mapping done.")
    return file_map


def download_files(files, download_folder="downloads"):
    if not os.path.exists(download_folder):
        os.mkdir(download_folder)

    try:
        logger.info("Starting Download!")

        for filename, uri in files.items():
            # print(x, files[x])
            logger.info(f"Getting the file : {filename}")
            file_path = os.path.join(download_folder, filename)

            if os.path.exists(file_path):
                logger.info(f"File {filename} already exists, moving on.")
                continue

            # getting file in chunks from uri.
            r = requests.get(uri, stream=True)
            r.raise_for_status()

            logger.info(f"Saving the file : {filename}")
            with open(file_path, "wb") as writeFile:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        writeFile.write(chunk)
            logger.info(f"File {filename} downloaded successfully!.")
            # break

        logger.info("Download complete!")

    except Exception as e:
        logger.critical(f"Pipeline Crashed: {e}")


def extract_zip(files, extractLoc="data", download_folder="downloads"):
    try:
        logger.info("Unzipping the Files!")
        if not os.path.exists(extractLoc):
            os.mkdir(extractLoc)

        for filename in files:
            filepath = os.path.join(download_folder, filename)

            if not os.path.exists(filepath):
                logger.info(f"File {filename} does not exist!.")

            # print(filepath,filename)
            new_path = filename.split(".")[0]
            new_path = os.path.join(extractLoc, new_path)

            # moving on if file already extracted.
            if os.path.exists(new_path):
                logger.info("Already unzipped, moving on.")
                continue

            logger.info(f"Extracting data from zip :: {filename}")
            # print(new_path)
            with zipfile.ZipFile(filepath, "r") as zf:
                zf.extractall(path=new_path)

            #     # print('welcome')
            logger.info(f"Extracted zip file : {filename}")
            # break

        logger.info("Unzipping done!.")
    except Exception as e:
        logger.critical(f"Pipeline Crashed: {e}")


def main():
    logger.info("Starting aws data pipeline!.")
    data_file_uris = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/202103-divvy-tripdata.zip",
    ]

    file_map = get_file_map(data_file_uris)
    download_files(file_map)
    extract_zip(file_map)
    logger.info("data extracted completed!.")


if __name__ == "__main__":
    main()
    # getFileNames()
