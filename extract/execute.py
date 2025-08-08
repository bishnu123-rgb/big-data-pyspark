import os
import sys
import time
import requests
from zipfile import ZipFile
import json
sys.path.append('/home/bishnu/ETL')
from utility.utility import setup_logger, format_seconds

logger = setup_logger("extract.log")

def download_file(url, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)
        return filename
    else:
        raise Exception("Failed to download file")

def extract_zip(zip_filename, output_dir):
    with ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    os.remove(zip_filename)

def fix_json_format(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)

    with open(json_file.replace(".json", "_fixed.json"), 'w') as f:
        for key, value in data.items():
            f.write(json.dumps({key: value}) + '\n')

    os.remove(json_file)

if __name__ == "__main__":
    start_time = time.time()

    if len(sys.argv) < 2:
        logger.error("Usage: python execute.py <output_dir>")
        sys.exit(1)

    EXTRACT_PATH = sys.argv[1]
    KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250807%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250807T071049Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=4c1289a121ebf33cd5bbad2f562593aebf444bb3d05029cc71f4f23b9b785e7e558dfce478dbc28978d5a8125d84581d0bc82d4ac9b3337aa27ee28eb1733c1d308817be7c7b0b1e1ffe50c2b0c1f30cb09f0b11907e8ce551a9ac26884dde9c9249fadadbbb54d9fbff8c98c7181efef7404e779a333527b7d7faf2252654809340f364566c584e05b133528f88a1efa025285cf0470ef6b2b7505d4f7b318adcc3987a19255af124f699a28fb328e553b0dc93cb1da8373620f5d4f2600f357df4d45fe3c2995e9e02feeb8098c36bbf4e538d55457d5816c79f24f9ae0f3037d0ece4b4501df4b3eb1506533110e2c8b3d932db00eca1e93bc456eb154523"

    try:
        zip_file = download_file(KAGGLE_URL, EXTRACT_PATH)
        extract_zip(zip_file, EXTRACT_PATH)
        fix_json_format(os.path.join(EXTRACT_PATH, "dict_artists.json"))
        logger.info("✅ Extraction and JSON fix complete.")
    except Exception as e:
        logger.error("❌ Extraction failed", exc_info=True)

    end_time = time.time()
    logger.info(f"🕒 Execution time: {format_seconds(end_time - start_time)}")
