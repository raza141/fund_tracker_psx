import re
import logging
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime, date

import pdfplumber
import pandas as pd

from scrapper import scrapper as sp


class DataReader:
    def __init__(self) -> None:
        self.data_folder = sp.Scrapper().create_or_get_folder()

    def get_files(self) -> dict[str, list[Path]]:
        dictionary = Path(self.data_folder)

        files_by_extension = {}
        # iter over all the files in the directory
        for file in dictionary.iterdir():
            if file.is_file():
                file_extension = file.suffix.lower()
                # Checking if the extension is in the dictionary
                if file_extension in files_by_extension:
                    files_by_extension[file_extension].append(file)
                else:
                    files_by_extension[file_extension] = [file]

        return files_by_extension

    def get_file_reader(self):
        readers: list = []
        for file_extension, files in self.get_files().items():
            for file in files:
                stem_file_name = file.stem
                stem_file_name = re.sub('[^A-Za-z_]', '', stem_file_name)

                if file_extension == '.csv':
                    file_path = file
                    reader = pd.read_csv
                    readers.append(reader)
                elif file_extension == '.xls':
                    if stem_file_name.startswith("fut"):
                        file_path = file
                        reader = pd.read_excel
                    else:
                        file_path = file
                        reader = pd.read_excel
                    readers.append(reader)
                elif file_extension == '.pdf':
                    file_path = file
                    reader = pdfplumber.open
                    readers.append(reader)
                elif file_extension == '.z':
                    self.read_zip_file()
                    file_path = file
                    reader = pd.read_csv
                    readers.append(reader)

        return readers

    def get_file_path(self):
        file_path = []
        for file_extension, files in self.get_files().items():
            for file in files:
                file_path.append(file)
        return file_path

    def read_zip_file(self):
        try:
            zip_files = self.get_files().get('.z', [])
            if not zip_files:
                raise FileNotFoundError("Zip file not found")

            for zip_file in zip_files:
                with ZipFile(zip_file) as zp:
                    zp.extractall(self.data_folder)
                    extracted_file_name = "closing11.lis"
                    extracted_file_path = self.data_folder / extracted_file_name
                    print(f"File '{zip_file.name}' extracted successfully")

            return extracted_file_path

        except FileNotFoundError as e:
            logging.warning(str(e))


class DataPreprocessor:

    @staticmethod
    def get_market_summary(file_path) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read and preprocess market summary data from a CSV file.

        Parameters:
        - file_path (Path): The path to the CSV file.

        Returns:
        - tuple[pd.DataFrame, pd.DataFrame]: Two DataFrames representing ready market and future market data.
        """
        try:
            df = pd.read_csv(file_path, header=None, sep='|')
            if df.empty:
                raise pd.errors.EmptyDataError("CSV file is empty")

            df = df.iloc[:, :10]
            df.columns = ['date', 'Symbol', 'Sector Code', 'Name', 'open', 'high', 'low', 'close', 'volume', 'LCDP']
            df['date'] = pd.to_datetime(df['date'])

            ready_market = df[(df['Sector Code'] != 40) & (df['Sector Code'] != 41)]
            future_market = df[(df['Sector Code'] == 40) | (df['Sector Code'] == 41)]

            return ready_market, future_market
        except pd.errors.EmptyDataError as e:
            logging.warning(f"EmptyDataError: {e}")
        except pd.errors.ParserError as e:
            logging.warning(f"ParserError: {e}")
        except Exception as e:
            logging.warning(f"An unexpected error occurred: {e}")

    @staticmethod
    def get_indhist(file_path):
        try: 
            file_name = Path(file_path).name  # Get the file name
            date_part = file_name.split('indhist')[-1].split('.')[0] 
            df = pd.read_excel(file_path)
            df = df[['SYMBOL', 'IDX WT %', 'ORD SHARES']]
            df['date'] = pd.to_datetime(date_part, format='%d-%b-%Y')
        except Exception as e:
            logging.warning(f"An unexpected error occurred: {e}")

        return df

    @staticmethod
    def get_omts(file_path):
        """
            This method receives a CSV file path and splits the data into two separate pd.DataFrames.

            Parameters:
                file_path (str): CSV file path to read.

            Returns:
                df1: The first processed DataFrame containing off-market BROKER TO BROKER TRADES.
                df2: The second processed DataFrame containing CROSS TRANSACTIONS BETWEEN CLIENT TO CLIENT & FINANCIAL INSTITUTIONS
            """
        try:
            df = pd.read_csv(file_path, skip_blank_lines=True, skiprows=4)
            # Find the index of the first row where any column is NaN
            separator_index = df[df.isnull().any(axis=1)].index[0]
            # Slice the DataFrame into two separate DataFrames using this index
            df1 = df.loc[: separator_index - 1].copy()
            df2 = df.loc[separator_index + 2:].copy()  # Skip the first row directly
            df1.reset_index(
                drop=True, inplace=True
            )  # reset the index to 0,1,2… after slicing
            df2.reset_index(drop=True, inplace=True)
            df1.columns = df1.columns.str.strip()
            df2.columns = df2.columns.str.strip()
            # split the 'MEMBER CODE' column literal string search
            df1[["0", "BUYER", "2", "SELLER"]] = df1["MEMBER CODE"].str.split(
                " ", expand=True
            )
            # Drop the unnecessary columns
            df1.drop(columns=["MEMBER CODE", "0", "2"], inplace=True)
            df1 = df1[
                [
                    "Date",
                    "SETTLEMENT DATE",
                    "BUYER",
                    "SELLER",
                    "SYMBOL CODE",
                    "COMPANY",
                    "TURNOVER",
                    "RATE",
                    "VALUES",
                ]
            ]  # reorder the columns
            return df1, df2
        except Exception as e:
            raise Exception(f"Error: {e}")

    @staticmethod
    def get_open_interest(file_path):
        """
        Extracts datatable from an `Excel file .xls` and returns a pandas DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame containing the fetched data.
        """
        try:
            sheet_names = pd.ExcelFile(file_path).sheet_names
            df = pd.read_excel(file_path, skiprows=2, sheet_name=sheet_names[1], header=0, index_col=0)
            df.columns = ['Symbol', 'Category', 'OI Contract', 'OI Volume', 'OI Value', 'FF of Script', "% FF"]
            df = df.iloc[3:, :].dropna()

            return df
        except Exception as e:
            print(f"Error Message: {e}")


if __name__ == '__main__':
    dd = DataPreprocessor()
    df1, df2 = dd.get_market_summary(DataReader().read_zip_file())
    print(df1)
