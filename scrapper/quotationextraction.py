import re
import logging
from pathlib import Path
from datetime import datetime

import pdfplumber
import pandas as pd
from tqdm import tqdm

logging.basicConfig(
    filename="pdf_extraction.log",
    level=logging.INFO,
    format="%(asctime)s -  %(levelname)s: %(message)s",
)


class PDFExtraction:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.target_pdf_file = self.open_pdf_file()
        self.target_page_nums_dict = self.extract_page_numbers()

    def open_pdf_file(self):
        try:
            return pdfplumber.open(self.file_path)  # Simplified file opening, directly using pdfplumber
        except FileNotFoundError:
            logging.error(f"The file {self.file_path} was not found.")
            raise

    def extract_page_numbers(self):
        """Find the Page Number where a specific pattern is found in the PDF."""
        target_table_pattern: list = [
            "SECTION 1: MACRO VIEW OF THE MARKET",
            "SECTION 5: BOARD MEETINGS",
            "SECTION 12: ALL SHARES INDEX REPORT (SECTOR WISE)",
            "SECTION 13: MAIN BOARD DATA FOR THE LAST 6 MONTHS",
        ]
        try:
            targeted_pages_nums = {}
            target_pdf = self.target_pdf_file.pages[1:]
            for page_num, page in tqdm(enumerate(target_pdf, start=1), ascii=True, desc="Extracting"):
                text = page.extract_text()
                for pattern in target_table_pattern:
                    if pattern in text:
                        if pattern not in targeted_pages_nums:
                            targeted_pages_nums[pattern] = []
                        targeted_pages_nums[pattern].append(page_num + 1)

            return targeted_pages_nums
        except Exception as e:
            logging.error(f"Error Message: {e}")

    def process_table(self):
        """
        This method dynamically extracts table data based on table names and page numbers.
        :return: Data Frame containing the table data extracted from the PDF.
        """
        try:
            table_data = {}
            for table_name, page_nums in self.target_page_nums_dict.items():
                if table_name == "SECTION 1: MACRO VIEW OF THE MARKET":
                    table_data[table_name] = self.extract_macro_view(page_nums)
                elif table_name == "SECTION 5: BOARD MEETINGS":
                    table_data[table_name] = self.extract_event_table(page_nums)
                elif table_name == "SECTION 12: ALL SHARES INDEX REPORT (SECTOR WISE)":
                    table_data[table_name] = self.extract_sector_table(page_nums)
                elif table_name == "SECTION 13: MAIN BOARD DATA FOR THE LAST 6 MONTHS":
                    table_data[table_name] = self.six_month_market_summary(page_nums)
            return table_data
        except Exception as e:
            logging.error(f"Error Message: {e}")

    def extract_macro_view(self, targeted_pages_nums: list) -> pd.DataFrame:
        try:
            loop_ending_pattern = "PUBLICLY ISSUED DEBT SECURITIES"
            metadata = self.target_pdf_file.metadata
            creation_date_string = metadata.get("CreationDate")
            creation_date = datetime.strptime(creation_date_string[2:10], "%Y%m%d").date()
            extracted_table = self.target_pdf_file.pages[targeted_pages_nums[0] - 1].extract_table()
            all_rows = []

            for row in extracted_table:
                table = [cell for cell in row if cell is not None]
                if any(loop_ending_pattern in cell for cell in table):
                    break
                all_rows.append(table)

            df = pd.DataFrame(all_rows)
            df.replace(",", "", regex=True, inplace=True)
            df.dropna(inplace=True)
            df.columns = ["Market", "Main Board", "Details", "GEM Board"]
            df = df.T
            df.drop("Details", axis=0, inplace=True)
            df.columns = df.iloc[0]
            df.drop(df.index[0], inplace=True)
            df.insert(0, "Date", creation_date)
            df.reset_index(inplace=True)
            df.insert(1, "c_ex_id", '1')
            
            return df
        except Exception as e:
            logging.error(f"Error Message: {e}")

    def extract_event_table(self, targeted_pages_nums: list) -> pd.DataFrame:
        """Extracts a table from the PDF based on the page numbers provided."""
        try:
            if len(targeted_pages_nums) == 1:
                target_table = self.target_pdf_file.pages[
                    targeted_pages_nums[0] - 1
                ].extract_table()  # -1 as page numbers start from 1 not 0
                tabledf = pd.DataFrame(target_table)
                tabledf.dropna(inplace=True)
                tabledf.columns = tabledf.iloc[0]
                tabledf.drop(tabledf.index[0], inplace=True)
                df = tabledf.iloc[:, 1:]
                date_col = df['Date'].astype(str)
                time_col = df['Time'].astype(str)
                df['Datetime'] = date_col + ' ' + time_col
                df['Datetime'] = pd.to_datetime(df['Datetime'])
                df.drop(columns=['Date', 'Time'], inplace=True)

            else:
                df = pd.DataFrame()
                for page in range(targeted_pages_nums[0] - 1, targeted_pages_nums[1]):
                    extracted_table_page = self.target_pdf_file.pages[page]
                    extracted_table = extracted_table_page.extract_table()
                    extracted_table = pd.DataFrame(extracted_table)
                    extracted_table.dropna(inplace=True)
                    
                    extracted_table.columns = extracted_table.iloc[0]
                    extracted_table.drop(extracted_table.index[0], inplace=True)
                    table = extracted_table.iloc[:, 1:]

                    df = pd.concat([df, table], axis=0)
            return df
        except Exception as e:
            logging.error(f"Error Message: {e}")
            return pd.DataFrame()

    def extract_sector_table(self, targeted_pages_nums: list) -> pd.DataFrame:
        """Extracts sector table from given PDF file for the specified pages.

        Parameters:
            targeted_pages_nums (list): List of page numbers to extract tables from.
        """
        try:
            final_df = pd.DataFrame()  # create an empty dataframe for initialization
            for page in range(targeted_pages_nums[0] - 1, targeted_pages_nums[1]):
                tables = self.target_pdf_file.pages[page].extract_tables()
                loop_ending_pattern = (
                    "SECTION 13: MAIN BOARD DATA FOR THE LAST 6 MONTHS"
                )
                data = []
                stop = False
                for table in tables:
                    for row in table:
                        cleaned_row = [
                            cell for cell in row if cell is not None
                        ]  # remove None values
                        if any(
                            loop_ending_pattern in cell for cell in cleaned_row
                        ):  # check if the pattern is found in the row and break the loop
                            stop = True
                            break
                        data.append(cleaned_row)
                    if stop:
                        break
                df = pd.DataFrame(data)
                df.dropna(inplace=True)
                df.columns = df.iloc[0]
                df = df.iloc[1:, 1:]
                # remove leading and trailing spaces
                df = df.apply(
                    lambda x: x.str.replace(",", "") if x.dtype == "object" else x
                )
                df = df.apply(
                    lambda x: x.str.strip() if x.dtype == "object" else x
                )  # remove commas
                df.columns = df.columns.str.replace("\n", " ")
                df["Sector Name"] = df["Sector Name"].str.replace("\n", " ")

                final_df = pd.concat([final_df, df], axis=0)

            return final_df

        except Exception as e:
            logging.error(f"Error Message: {e}")

    def six_month_market_summary(self, targeted_pages_nums: list) -> pd.DataFrame:
        """
        Extracts the six-month market summary from a given PDF file.
        Args:
            targeted_pages_nums (List[int]): A list of page numbers to extract tables from.
        Returns:
            pandas.DataFrame: A DataFrame containing the extracted market summary data.
        """

        try:
            tables = self.target_pdf_file.pages[targeted_pages_nums[0] - 1].extract_table()

            target_table = []
            start_appending = False
            for table in tables:
                if (
                    table[0] is not None
                    and "SECTION 13: MAIN BOARD DATA FOR THE LAST 6 MONTHS" in table[0]
                ):
                    start_appending = True
                if start_appending:
                    clean_table = [
                        cell for cell in table if cell is not None
                    ]  # remove empty cells (None values)
                    clean_table = [
                        re.sub(r"\s+", " ", clean_table) for clean_table in clean_table
                    ]  # remove leading and trailing spaces
                    target_table.append(clean_table)

                    if "SECTION 14: DEFAULTER SEGMENT" in clean_table:
                        break

            df = pd.DataFrame(
                target_table,
                columns=[
                    "Month at the Close",
                    "Listed Capital(million)",
                    "Market Capitalization(million)",
                    "Turnover In Ready MRKT",
                    "Turnover In Future MRKT",
                    "KSE 100 Index",
                    "KSE All Share",
                ],
            )
            df.dropna(inplace=True)
            df.replace(",", "", regex=True, inplace=True)
            return df
        except Exception as e:
            logging.error(f"Error Message: {e}")


def get_quotes(file_path):
    summary = PDFExtraction(file_path).process_table()
    for table_name, table in summary.items():
        print(table)


if __name__ == "__main__":
    get_quotes("/Volumes/PSXDatabase/psxkoyfin/scrapper/DataWarehouse/quote2024-01-23.pdf")

