import logging
import requests
from pathlib import Path
from datetime import date
from typing import List


class Scrapper:
    def __init__(self, config_date: str = None, config_path: str = "DataWarehouse"):
        """
        Initialize the Scrapper object.

        Parameters:
        - config_date: date to scrape the files (defaults is today)
        - storage_path (str): Path to the storage folder.
        """
        self.storage_path = Path(config_path)
        self.config_data = config_date or str(date.today())
        self.file_map = {
            "mkt_summary": [".Z"],
            "omts": [".csv"],
            "fut_opn_int": [".xls"],
            "quote": [".pdf"],
            "indhist": [".xls"]
        }
        self.base_url = "https://dps.psx.com.pk/download"
        self.storage_location = self.create_or_get_folder()
        self.setup_logging()

    def create_or_get_folder(self) -> Path:
        """
        Create or get the storage folder,
        Create the logs' folder.


        Returns:
        - Path: Path to the storage folder.
        """
        self.storage_path.mkdir(parents=True, exist_ok=True)
        logs_dir = self.storage_path / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return self.storage_path.resolve()

    def setup_logging(self):
        """
        Set up logging for the scraper with the specified storage location.
        """
        logging.basicConfig(
            filename=self.storage_location/"logs"/"scraper.log", level=logging.INFO,
            format="%(asctime)s - %(levelname)s: %(message)s"
        )

    def generate_url_links(self):
        """
        Generate a list of download URLs and download the files.

        Returns:
        - List[str]: List of downloaded file names.
        """
        full_url_links = []
        for name, exts in self.file_map.items():
            for ext in exts:
                full_url = f"{self.base_url}/{name}/{self.config_data}{ext}"
                full_url_links.append(full_url)
        self._download_files(full_url_links)

    def _download_files(self, urls: List[str]):
        """
        Downloads files from the given list of URLs and logs the success and failure of each download.

        Args:
            urls (List[str]): A list of URLs to download files from.

        Returns:
            None
        """

        downloaded_files = []
        failed_files = []
        for url in urls:
            try:
                response = requests.get(url)
                response.raise_for_status()
                file_name = "".join((url.split('/')[4:]))
                file_loc = self.storage_location / file_name
                with open(file_loc, 'wb') as file:
                    file.write(response.content)
                downloaded_files.append(file_name)
            except requests.HTTPError as http_err:
                failed_files.append(url)
                logging.info(f"HTTP error occurred: {http_err} - {url}")
            except Exception as err:
                failed_files.append(url)
                logging.info(f"Other error occurred: {err} - {url}")
        logging.info(f"Downloaded {len(downloaded_files)} files successfully: {downloaded_files}")
        logging.info(f"Failed to download {len(failed_files)} files: {failed_files}")


if __name__ == "__main__":
    scrapper = Scrapper("2024-01-18")
    scrapper.generate_url_links()
