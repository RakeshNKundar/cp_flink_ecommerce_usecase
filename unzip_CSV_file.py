import os
import zipfile
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

CSV_PATH = os.getenv('CSV_PATH')

class ZipHandler(FileSystemEventHandler):
    def __init__(self, extract_to):
        self.extract_to = extract_to
        os.makedirs(extract_to, exist_ok=True)

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".zip"):
            print(f"Detected new ZIP: {event.src_path}")
            self.unzip_file(event.src_path)

    def unzip_file(self, zip_path):
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.extract_to)
                print(f"Extracted {zip_path} → {self.extract_to}")
        except zipfile.BadZipFile:
            print(f"Error: {zip_path} is not a valid ZIP file.")

def watch_directory(watch_dir, extract_to):
    event_handler = ZipHandler(extract_to)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=False)
    observer.start()

    print(f"Watching directory: {watch_dir}")
    try:
        while True:
            pass  # Keep running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    watch_directory("zipped_files", CSV_PATH)
