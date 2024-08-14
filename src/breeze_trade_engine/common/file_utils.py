from datetime import datetime
import logging
import pandas as pd
import os


class FileWriterMixin:

    def write_to_csv(self, data):
        """
        Function to write data to a CSV file
        """

        if self.file is None:
            self.logger.error("No csv file path provided.")
            return
        # Convert data to a Pandas DataFrame (if needed)
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])

        # Write DataFrame to CSV (using pandas)
        df.to_csv(
            self.file, mode="a", index=False, header=not os.path.exists(self.file)
        )
        self.logger.info(f"Data written to {self.file}.")

    def write_to_parquet(self, delete_csv=True):
        """
        Function to write data to a Parquet file
        """

        if self.file is None:
            self.logger.error("No file path provided.")
            return

        file_prefix = self.file.split("/")[-1].split(".")[0]
        parquet_filename = f"{file_prefix}.parquet"

        # Convert CSV to Parquet
        if os.path.exists(self.file):
            df = pd.read_csv(self.file)
            df.to_parquet(parquet_filename, index=False)

            # Verify Parquet file creation
            if os.path.exists(parquet_filename):
                try:
                    parquet_df = pd.read_parquet(parquet_filename)
                    csv_rows = len(pd.read_csv(self.file))
                    parquet_rows = len(parquet_df)
                    if csv_rows == parquet_rows:
                        self.logger.info(
                            f"CSV to Parquet conversion successful. {csv_rows} rows in both files.\
                                {parquet_filename} and {self.file}."
                        )
                    else:
                        self.logger.error(
                            f"CSV and Parquet row counts mismatch. CSV: {csv_rows}, Parquet: {parquet_rows}"
                        )
                except Exception as e:
                    self.logger.error(f"Error reading Parquet file: {e}")
                    return

            # Delete CSV file older than 7 days
            if delete_csv:
                today = datetime.now().date()
                path = os.path.dirname(self.file)
                for filename in os.listdir(path):
                    file_prefix = self.file.split("/")[-1].split(".")[0]
                    file_date = file_prefix.split("_")[-1]
                    file_date = datetime.strptime(file_date, "%Y-%m-%d")
                    if (today - file_date).days > 7:
                        os.remove(filename)
                        logging.info(f"Deleted CSV file: {filename}")
