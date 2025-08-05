import os, sys, requests
from zipfile import ZipFile


def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)

    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded zip file: {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file: Status code {response.status_code}")


def extract_zip_file(zip_filename, output_dir):

    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)

    print(f"Extract files written to: {output_dir}")
    print("Removing the zip file")
    os.remove(zip_filename)


def fix_json_dict(output_dir):
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
        print(
            f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json"
        )

    print("Removing the original file")
    os.remove(file_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Exame Usage:")
        print("python3 execute.py /home/Data/Extraction")
    else:
        try:
            print("Starting Extration Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/yamaerenay/spotify-dataset-19212020-600k-tracks"
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(zip_filename, EXTRACT_PATH)
            fix_json_dict(EXTRACT_PATH)
            print("Extraction Successfully Completed!! :))")
        except Exception as e:
            print(f"Error: {e}")
