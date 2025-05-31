import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# --- Logging function ---
def write_log(message, log_file="log_file.txt"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")

# --- Extraction functions ---
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process)
    return df

def extract_from_xml(file_to_process):
    records = []  # Use a list to collect rows
    tree = ET.parse(file_to_process)
    root = tree.getroot()

    for row in root:
        car_model = row.find("car_model").text
        year_of_manufacture = int(row.find("year_of_manufacture").text)
        price = round(float(row.find("price").text), 2)
        fuel = row.find("fuel").text

        records.append({
            "car_model": car_model,
            "year_of_manufacture": year_of_manufacture,
            "price": price,
            "fuel": fuel
        })

    # Convert to DataFrame once at the end
    df = pd.DataFrame(records)
    return df

# --- ETL process with logging ---
def etl_process(file_path, extract_func, log_file="log_file.txt"):
    try:
        write_log(f"Started extraction from {file_path}", log_file)
        df = extract_func(file_path)
        write_log(f"Extraction successful: {len(df)} records loaded", log_file)

        # Transformation: round price column to 2 decimals if present
        if 'price' in df.columns:
            df['price'] = df['price'].round(2)
            write_log("Transformation applied: price rounded to 2 decimals", log_file)

        # Load to CSV
        target_file = "transformed_data.csv"
        df.to_csv(target_file, index=False)
        write_log(f"Loading successful: data saved to {target_file}", log_file)

        write_log("ETL process completed successfully", log_file)
    except Exception as e:
        write_log(f"ETL process failed: {str(e)}", log_file)

# --- Main execution for testing ---
if __name__ == "__main__":
    # Replace these filenames with your actual test files
    test_files = {
        "CSV": ("used_car_prices1.csv", extract_from_csv),
        "JSON": ("used_car_prices1.json", extract_from_json),
        "XML": ("used_car_prices1.xml", extract_from_xml)
    }

    for file_type, (filename, func) in test_files.items():
        print(f"Running ETL for {file_type} file: {filename}")
        etl_process(filename, func)
        print(f"Done ETL for {file_type}\n")