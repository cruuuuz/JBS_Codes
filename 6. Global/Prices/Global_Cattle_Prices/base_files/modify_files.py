import shutil
import os
import openpyxl

def copy_and_convert_excel(source_path, dest_path):
    XLS_EXTENSION = '.xlsx'
    XLSM_EXTENSION = '.xlsm'
    """Copies an Excel file and converts it to .xlsx format."""
    try:
        shutil.copy(source_path, dest_path)
        workbook = openpyxl.load_workbook(dest_path)  # Load only after copy
        dest_path_xlsx = dest_path.replace(XLSM_EXTENSION, XLS_EXTENSION)
        workbook.save(dest_path_xlsx)
        os.remove(dest_path) #Remove the xlsm file after creating xlsx

    except FileNotFoundError:
        print(f"Error: Source file not found: {source_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

class Modify_files:
    @staticmethod
    def modify_files(path: str, base_dir_path: str, base_file_name: str, currency_file_name: str):
        """Main function to copy and convert the Excel files."""

        # Create the destination directory if it doesn't exist
        os.makedirs(path, exist_ok=True)


        base_file_source = os.path.join(base_dir_path, base_file_name)
        base_file_dest = os.path.join(path, base_file_name)
        copy_and_convert_excel(base_file_source, base_file_dest)

        base_currency_source = os.path.join(base_dir_path, currency_file_name)
        base_currency_dest = os.path.join(path, currency_file_name)
        copy_and_convert_excel(base_currency_source, base_currency_dest)


