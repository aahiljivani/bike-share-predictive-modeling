import requests
import io
import zipfile
import pandas as pd
import chardet

class BikeShareDataLoader:
    """
    Loads and processes Toronto Bike Share ridership data from CKAN API resource metadata.

    Attributes:
        resources (list): List of resource metadata dictionaries from CKAN.

    Methods:
        get_response(url): Makes a GET request to the given URL.
        get_metadata(): Returns metadata from CKAN API.
        get_files(year): Loads all supported files for the given year and returns a list of DataFrames.
        create_df(year): Returns a combined DataFrame for the given year.
    """

    def __init__(self, package_url):
        self.package_url = package_url
        self.resources = self.get_metadata()

    def get_response(self, url):
        return requests.get(url, timeout=15)

    def get_metadata(self):
        response = self.get_response(self.package_url)
        data = response.json()
        assert data.get("success", False) is True
        return data["result"]["resources"]
    
    def extract_valid_dataframes_from_zip(self, zip_content, year):
        dfs = []
        year_str = str(year)
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            for filename in z.namelist():
                # Only process files whose name contains the specified year
                if filename.endswith(('.csv', '.xlsx')) and year_str in filename:
                    with z.open(filename) as f:
                        if filename.endswith('.csv'):
                            # Read a small sample to detect encoding
                            sample = f.read(2000)
                            detected = chardet.detect(sample)
                            encoding = detected.get('encoding', 'utf-8')
                            # Fallback if ascii encoding is detected
                            if encoding and encoding.lower() == 'ascii':
                                encoding = 'cp1252'
                            f.seek(0)
                            df = pd.read_csv(f, encoding=encoding)
                        elif filename.endswith('.xlsx'):
                            df = pd.read_excel(f, engine="openpyxl")
                        dfs.append(df)
        return dfs

    def get_files(self, year):
        dfs = []
        year_str = str(year)
        for value in self.resources:
            url_lower = value["url"].lower()
            # Skip if the resource URL does not include the year, contains "readme", or is not a supported format.
            if year_str not in url_lower or "readme" in url_lower or not url_lower.endswith(('.zip', '.xlsx', '.csv')):
                continue

            url = value["url"]
            try:
                r = self.get_response(url)
                fmt = value["format"].lower()
                if fmt == "zip":
                    dfs.extend(self.extract_valid_dataframes_from_zip(r.content, year))
                elif fmt == "xlsx":
                    df = pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
                    dfs.append(df)
                elif fmt == "csv":
                    sample = r.content[:2000]
                    detected = chardet.detect(sample)
                    encoding = detected.get('encoding', 'utf-8')
                    if encoding and encoding.lower() == 'ascii':
                        encoding = 'cp1252'
                    df = pd.read_csv(io.BytesIO(r.content), encoding=encoding)
                    dfs.append(df)
            except Exception as e:
                print(f"Failed to process {url}: {e}")
        return dfs

    def create_df(self, year):
        data = self.get_files(year)
        if not data:
            print(f"No valid data files found for year {year}.")
            return pd.DataFrame()
        return pd.concat(data, ignore_index=True)