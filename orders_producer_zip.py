import os
import csv
import uuid
import time
import zipfile
import random
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

CSV_TEMP_PATH = os.getenv('CSV_TEMP_PATH')       # temporary CSV storage
ZIP_OUTPUT_PATH = os.getenv('ZIP_OUTPUT_PATH') # final ZIP directory

os.makedirs(CSV_TEMP_PATH, exist_ok=True)
os.makedirs(ZIP_OUTPUT_PATH, exist_ok=True)

def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "product_id": f"product-{random.randint(1, 50)}",
        "quantity": random.randint(1, 10),
        "order_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    }

def produce_orders_csv():
    fieldnames = ["order_id", "product_id", "quantity", "order_ts"]

    try:
        while True:

            # Create 3 CSV files
            csv_files = []
            for i in range(3):
                csv_filename = f"orders_{uuid.uuid4()}.csv"
                csv_path = os.path.join(CSV_TEMP_PATH, csv_filename)

                with open(csv_path, mode="w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow(generate_order())

                csv_files.append(csv_path)
                print(f"Created CSV: {csv_path}")

            # Create ZIP filename
            zip_filename = f"batch_{uuid.uuid4()}.zip"
            zip_path = os.path.join(ZIP_OUTPUT_PATH, zip_filename)

            # Add the CSVs into the ZIP
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for csv_file in csv_files:
                    zipf.write(csv_file, arcname=os.path.basename(csv_file))

            print(f"ZIP created: {zip_path}")

            # Remove temp CSV files
            for csv_file in csv_files:
                os.remove(csv_file)

            # Wait before generating next ZIP batch
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopped writing CSV+ZIP batches.")


if __name__ == "__main__":
    produce_orders_csv()