import pandas as pd
import asyncio
import aiofiles
import os
import time


class AsyncRollingDataFrame:
    def __init__(
        self, folder_name, filename="data.csv", max_rows=1000, keep_history=True
    ):
        self.folder_name = folder_name
        self.filename = filename
        self.filepath = os.path.join(folder_name, filename)
        self.keep_history = keep_history
        self.max_rows = max_rows
        self.df1 = pd.DataFrame()
        self.df2 = pd.DataFrame()
        self.current_df = self.df1
        self.lock = asyncio.Lock()

        # Ensure the folder exists. Do not overwrite existing files.
        os.makedirs(folder_name, exist_ok=True)

    async def add_data(self, new_data):
        async with self.lock:
            if isinstance(new_data, dict):
                new_data = [new_data]
            new_df = pd.DataFrame(new_data)
            self.current_df = pd.concat(
                [self.current_df, new_df], ignore_index=True
            )
            if len(self.current_df) >= self.max_rows:
                frame_to_flush = self.current_df
                if self.keep_history:
                    asyncio.create_task(self.flush_to_disk(frame_to_flush))
                self.switch_dataframes()

    async def flush_to_disk(self, frame_to_flush):
        file_exists = os.path.exists(self.filepath)
        mode = "a" if file_exists else "w"
        async with aiofiles.open(self.filepath, mode=mode) as f:
            csv_data = frame_to_flush.to_csv(
                index=False, header=not file_exists
            )
            await f.write(csv_data)

    def switch_dataframes(self):
        if self.current_df is self.df1:
            self.current_df = self.df2
            self.df1 = pd.DataFrame()
        else:
            self.current_df = self.df1
            self.df2 = pd.DataFrame()

    async def load_complete_dataframe(self):
        if not os.path.exists(self.filepath):
            return pd.DataFrame()

        async with self.lock:
            return pd.read_csv(self.filepath)

    async def close(self):
        await self.flush_to_disk(self.current_df)
        self.df1 = pd.DataFrame()
        self.df2 = pd.DataFrame()
        self.current_df = self.df1
