from datetime import datetime
import pandas as pd
import os
from pathlib import Path


async def write_to_csv(data, file, logger, mode="a"):
    """
    Function to write data to a CSV file
    """

    if file is None:
        logger.error("No csv file path provided.")
        return
    # Convert data to a Pandas DataFrame (if needed)
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame([data])

    # Write DataFrame to CSV (using pandas)
    file_dir, file_name = os.path.split(file)
    file_dir = Path(file_dir)
    file_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        file_dir / file_name,
        mode=mode,
        index=False,
        header=not os.path.exists(file),
    )
    logger.info(f"Data written to {file}.")


async def write_to_parquet(file, logger, delete_csv=True):
    """
    Function to write data from `file` csv file to a Parquet file
    """

    if file is None:
        logger.error("No file path provided.")
        return

    file_dir, file_name = os.path.split(file)
    file_dir = Path(file_dir)
    file_dir.mkdir(parents=True, exist_ok=True)
    file_name = file_dir / (file_name.split(".")[0] + ".parquet")
    # Convert CSV to Parquet
    if os.path.exists(file):
        df = pd.read_csv(file)
        df.to_parquet(file_dir / file_name, index=False)

        # Verify Parquet file creation
        if os.path.exists(file_dir / file_name):
            try:
                parquet_df = pd.read_parquet(file_dir / file_name)
                csv_rows = len(pd.read_csv(file))
                parquet_rows = len(parquet_df)
                if csv_rows == parquet_rows:
                    logger.info(
                        f"CSV to Parquet conversion successful. {csv_rows} rows in both files.\
                            {file_dir / file_name} and {file}."
                    )
                else:
                    logger.error(
                        f"CSV and Parquet row counts mismatch. CSV: {csv_rows}, Parquet: {parquet_rows}"
                    )
            except Exception as e:
                logger.error(f"Error reading Parquet file: {e}")
                return

        # Delete CSV file older than 7 days
        if delete_csv:
            today = datetime.now().date()
            path = os.path.dirname(file)
            for filename in os.listdir(path):
                file_prefix = file.split("/")[-1].split(".")[0]
                file_date = file_prefix.split("_")[-1]
                file_date = datetime.strptime(file_date, "%Y-%m-%d")
                if (today - file_date).days > 7:
                    os.remove(filename)
                    logger.info(f"Deleted CSV file: {filename}")
