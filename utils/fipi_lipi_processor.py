import os
import pandas as pd
import re
from datetime import datetime
from pathlib import Path

class FIPIDataProcessor:
    def __init__(self, directory, file_type:str ='fipi'):
        """
        Initialize the FIPIDataProcessor with the directory containing the CSV files.
        """
        self.directory = directory
        self.file_type = file_type

    def scan_files(self):
        """
        Scan the directory and return a list of file paths for CSV files across all subdirectories (like 'fipi/2019', 'fipi/2020', etc.).
        """
        if self.directory is None:
            print("Error: Directory path is not set.")
            return []
        
        all_csv_files = []

        try:
            # Check if the directory exists
            if not os.path.exists(self.directory):
                print("Error: Directory does not exist.")
                return []
            
            # Iterate over each subdirectory (like 'fipi/2019', 'fipi/2020')
            for year_dir in os.listdir(self.directory):
                year_path = Path(self.directory) / year_dir
                if year_path.is_dir():  # Only process directories
                    print(f"Scanning directory: {year_dir} : {year_path}")
                    with os.scandir(year_path) as files:
                        # Collect paths for all CSV files in each year directory
                        csv_files = [file.path for file in files if file.is_file() and file.path.endswith('.csv')]
                        all_csv_files.extend(csv_files)  # Add to the list of all CSV files
            
            if not all_csv_files:
                print("Error: No CSV files found in the directory.")
            
            return all_csv_files

        except FileNotFoundError:
            print("Error: Directory not found.")
            return []
        except PermissionError:
            print("Error: Permission denied to access the directory.")
            return []
        except OSError as e:
            print(f"Error: An OS error occurred: {e}")
            return []

    def extract_date_from_filename(self, file_path):
        """
        Extract the date from the filename. The format is expected to be like '01-03-2024fipi.csv'.
        """
        file_name = (os.path.basename(file_path)).lower()
        # print(file_name)
        date_str = file_name.split(self.file_type)[0]  # Extract date part
        try:
            # Convert the extracted string to a datetime object
            date_fipi = datetime.strptime(date_str, '%d-%m-%Y')

            return date_fipi
        except ValueError:
            print(f"Error parsing date from file: {file_path}")
            return None

    def clean_numeric_re(self, value):
        """
        Clean and convert numeric strings with parentheses to negative numbers, remove commas, and convert to float.
        """
        if pd.isna(value):
            return None
        # Handle parentheses for negative numbers
        value = re.sub(r'\((.*?)\)', r'-\1', str(value))
        # Remove commas and any non-numeric characters except minus and decimal points
        value = re.sub(r'[^\d.-]', '', value)
        try:
            return float(value)
        except ValueError:
            return None

    def clean_dataframe(self, df, date_fipi):
        """
        Clean the DataFrame by applying the cleaning function to relevant columns and inserting the date.
        """
        # Filter rows where SEC CODE and SECTOR NAME are not NaN
        df_clean = df[~df['SEC CODE'].isna() & ~df['SECTOR NAME'].isna()]
        # Filter out rows where CLIENT TYPE is a blank space
        df_clean = df_clean[df_clean['CLIENT TYPE'] != ' ']

        # Columns that need to be cleaned
        cols_to_convert = ['BUY VOLUME', 'BUY VALUE', 'SELL VOLUME', 'SELL VALUE', 'NET VOLUME', 'NET VALUE', 'USD']

        # Apply the cleaning function to each column
        for col in cols_to_convert:
            df_clean[col] = df_clean[col].apply(self.clean_numeric_re)

        # Insert the extracted date as a new column
        df_clean.insert(0, 'Date', date_fipi)
        return df_clean

    def process_files(self):
        """
        Process all CSV files in the directory, cleaning and extracting relevant data.
        """
        csv_files = self.scan_files()
        all_data = pd.DataFrame()  # Empty DataFrame to store all processed data
        
        # Loop through each file
        for file_path in csv_files:
            # print(f"Processing file: {file_path}")
            date_fipi = self.extract_date_from_filename(file_path)  # Extract date
            if date_fipi:
                df = pd.read_csv(file_path)  # Read the CSV file into a DataFrame
                df_clean = self.clean_dataframe(df, date_fipi)  # Clean the data
                all_data = pd.concat([all_data, df_clean], ignore_index=True)  # Append cleaned data
        
        return all_data

if __name__ == "__main__":
    directory = "data/fipi"  # Directory containing your FIPI CSV files
    processor = FIPIDataProcessor(directory)

    # Process and get the cleaned data
    cleaned_data = processor.process_files()
    print(cleaned_data.head())  # Display the first few rows of the cleaned data
