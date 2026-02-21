import os
import logging
import zipfile
import tarfile
import gdown
import magic
import hashlib

URL = "https://drive.google.com/file/d/1MYk9WK_0K_out54YaNUl41p2nzL24Ta8/view"

PARENT_DIR = "dataset"
ARCHIVE_DIR = os.path.join(PARENT_DIR, "archives")
EXTRACT_DIR = os.path.join(PARENT_DIR, "extracted")
LOG_FILE = os.path.join(PARENT_DIR, "process.log")

REMOVE_EXTENSIONS = [".txt", ".md", ".url", ".DS_Store"]


# --------------------------------------------------
# Setup
# --------------------------------------------------

def setup():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    logging.info("Process started")


# --------------------------------------------------
# Download
# --------------------------------------------------

def download_file():
    output = os.path.join(ARCHIVE_DIR, "archive")

    logging.info("Downloading file")

    path = gdown.download(URL, output, fuzzy=True)

    logging.info(f"Downloaded to {path}")

    return path


# --------------------------------------------------
# Detect file type
# --------------------------------------------------

def detect_archive_type(path):

    try:
        mime = magic.from_file(path, mime=True)

        if mime == "application/zip":
            return "zip"

        if mime in ["application/x-tar"]:
            return "tar"

        if mime in ["application/gzip", "application/x-gzip"]:
            return "tar.gz"

    except Exception as e:
        logging.warning(f"MIME detection failed: {e}")

    # fallback: signature
    with open(path, "rb") as f:
        sig = f.read(8)

    if sig.startswith(b"PK"):
        return "zip"

    if sig.startswith(b"\x1f\x8b"):
        return "tar.gz"

    return "unknown"

def safe_extract_zip(path):

    with zipfile.ZipFile(path) as z:

        for member in z.infolist():

            extracted_path = os.path.join(EXTRACT_DIR, member.filename)

            if not os.path.realpath(extracted_path).startswith(os.path.realpath(EXTRACT_DIR)):
                raise Exception("Zip Slip detected")

        z.extractall(EXTRACT_DIR)


def safe_extract_tar(path, mode):

    with tarfile.open(path, mode) as tar:

        for member in tar.getmembers():

            member_path = os.path.join(EXTRACT_DIR, member.name)

            if not os.path.realpath(member_path).startswith(os.path.realpath(EXTRACT_DIR)):
                raise Exception("Tar Path Traversal detected")

        tar.extractall(EXTRACT_DIR)


def extract_archive(path):

    t = detect_archive_type(path)

    logging.info(f"Detected archive type: {t}")

    if t == "zip":
        safe_extract_zip(path)

    elif t == "tar":
        safe_extract_tar(path, "r:")

    elif t == "tar.gz":
        safe_extract_tar(path, "r:gz")

    else:
        raise Exception("Unsupported archive")

    logging.info("Extraction finished")

def cleanup_files():

    removed = 0

    for root, dirs, files in os.walk(EXTRACT_DIR):

        for file in files:

            path = os.path.join(root, file)

            if any(file.endswith(ext) for ext in REMOVE_EXTENSIONS):

                os.remove(path)

                logging.info(f"Removed {path}")

                removed += 1

    logging.info(f"Cleanup finished, removed {removed} files")
def sha256(path):

    h = hashlib.sha256()

    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)

    return h.hexdigest()
def main():

    setup()

    archive = download_file()

    logging.info(f"SHA256: {sha256(archive)}")

    extract_archive(archive)

    cleanup_files()

    logging.info("Process completed")


if __name__ == "__main__":
    main()