import logging
from tqdm import tqdm
from glob import glob
from datetime import date
import time 
from config.project_config import DAILY_MARKET_DATA_DOWNLOAD_PATH

from scrapper import scrapper as sp
from scrapper import datareader as dr
from scrapper import quotationextraction as qe


class Execution:
    def __init__(self, target_date: str = str(date.today()), files_deletion=True) -> None:
        """
        Initializes the object with a data reader and sets the file path attribute.
        """
        self.date = target_date
        self.download_directory = sp.Scrapper().create_or_get_folder()
        if not self._check_existing_files():
            # download the files
            self.today_downloads = sp.Scrapper(self.date).generate_url_links()

        self.file_path: dict = dr.DataReader().get_files()
        self.data_reader = dr.DataPreprocessor()
        self.files_deletion = files_deletion
        self._setup_logging()

    def _setup_logging(self):
        """
        Set up logging for the scraper with the specified storage location.
        """
        logging.basicConfig(
            filename=self.download_directory/"logs"/"scraper.log", level=logging.INFO,
            format="%(asctime)s - %(levelname)s: %(message)s"
        )

    def _check_existing_files(self) -> bool:
        """
        check for existing files in the download directory based on the specified date.
        
        :return: bool indicated whether existing files were found
        """
        file_pattern = f"*{self.date}*"
        existing_files = list(self.download_directory.glob(file_pattern))
        if len(existing_files) > 0:
            logging.info(f"Found {len(existing_files)} existing files")
        else:
            logging.info(f"No existing files found for {self.date}")

        return len(existing_files) > 0

    def _delete_files_in_directory(self):
        """
        Deletes all files in the DataWarehouse directory.
        """
        try:
            for file in (self.download_directory / DAILY_MARKET_DATA_DOWNLOAD_PATH).glob("*"):
                logging.info(f"Deleting file: {file}")
                if file.is_file():
                    file.unlink()
            logging.info("All files deleted successfully.")
        except Exception as e:
            logging.error(f"Error while deleting files: {e}")

    def execution_by_extension(self):
        """
        Iterates through each file based on its extension and executes the corresponding 
        function to extract and process the data. Returns a list of dataframes.
        """
        df_list = []
        try:
            for extension, files in tqdm(self.file_path.items()):
                logging.info("Executing " + extension)

                if extension == ".z":
                    # Decompress the Unix-documents which is in .Z
                    extracted_file_path = dr.DataReader().read_zip_file()
                    # Calling the right function based on an extension
                    df = self.data_reader.get_market_summary(extracted_file_path)
                    df_list.append(df)

                elif extension == ".csv":
                    for file in files:
                        df = self.data_reader.get_omts(file)
                        df_list.append(df)

                elif extension == ".xls":
                    for file in files:
                        # Stem the file name to get the right file name
                        if file.stem.startswith("fut_opn_int"):
                            df = self.data_reader.get_open_interest(file)
                        else:
                            df = self.data_reader.get_indhist(file)
                        df_list.append(df)
                elif extension == ".pdf":
                    # calling the right function based on an extension and extracting the dict
                    for file in files:
                        df = qe.get_quotes(file)
                        df_list.append(df)
        
        except Exception as e:
            logging.info(f"Error while executing {extension}: {e}")
            
        # After processing, delete the files
        time.sleep(2)
        if self.files_deletion:
            self._delete_files_in_directory()
        
        # Display output
        return df_list


if __name__ == '__main__':
    ee = Execution("2024-02-19")
    print(ee.execution_by_extension())
    pdf
