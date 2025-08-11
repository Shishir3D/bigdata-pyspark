import os, sys, requests
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utility.utility import setup_logging

def download_zip_file(logger, url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)

    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file: {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file: Status code {response.status_code}")


def extract_zip_file(logger, zip_filename, output_dir):

    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)

    logger.info(f"Extract files written to: {output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)


def fix_json_dict(logger, output_dir):
    import json

    file_path = os.path.join(output_dir, "dict_artists.json")

    with open(file_path, "r") as f:
        data = json.load(f)

    with open(
        os.path.join(output_dir, "fixed_da.json"), "w", encoding="utf-8"
    ) as f_out:
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
        logger.info(
            f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json"
        )

    logger.info("Removing the original file")
    os.remove(file_path)


if __name__ == "__main__":
    logger = setup_logging("extract.log")

    if len(sys.argv) < 2:
        logger.info("Extraction path is required")
        logger.info("Exame Usage:")
        logger.info("python3 execute.py /home/Data/Extraction")
    else:
        try:
            logger.info("Starting Extration Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks"
            zip_filename = download_zip_file(logger, KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(logger, zip_filename, EXTRACT_PATH)
            fix_json_dict(logger, EXTRACT_PATH)
            logger.info("Extraction Successfully Completed!! :))")
        except Exception as e:
            logger.error(f"Error: {e}")
