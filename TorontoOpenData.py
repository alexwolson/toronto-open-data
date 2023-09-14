from pathlib import Path
from ckanapi import RemoteCKAN
from ckanapi.errors import CKANAPIError

import pandas as pd

from tqdm import tqdm

import csv

import wget


class TorontoOpenData:
    def __init__(self, api_key: str = None):
        """
        :param api_key: The API key for accessing the Toronto Open Data portal, defaults to None
        :type api_key: str, optional
        """
        self.ckan = RemoteCKAN('https://ckan0.cf.opendata.inter.prod-toronto.ca', apikey=api_key)
        self.smart_return_filetypes = [
            'csv',
            'docx',
            'gpkg',
            'geojson',
            'jpeg',
            'json',
            'kml',
            'pdf',
            'sav',
            'shp',
            'txt',
            'xlsm',
            'xlsx',
            'xml',
            'xsd'
        ]

    def list_all_datasets(self, as_frame: bool = True):
        """
        Lists all available datasets.

        :param as_frame: Whether to return the result as a Pandas DataFrame, defaults to True
        :type as_frame: bool, optional
        :return: List of datasets
        :rtype: list or pd.DataFrame
        """
        result = self.ckan.action.package_list()

        if as_frame:
            return pd.DataFrame(result)
        else:
            return result

    def search_datasets(self, query: str, as_frame: bool = True):
        """
        Searches datasets by a given keyword.

        :param query: Keyword to search for
        :type query: str
        :param as_frame: Whether to return the result as a Pandas DataFrame, defaults to True
        :type as_frame: bool, optional
        :return: List of datasets that match the query
        :rtype: list or pd.DataFrame
        """
        result = self.ckan.action.package_search(q=query)

        if 'results' in result:
            if as_frame:
                return pd.DataFrame(result['results'])
            else:
                return result['results']
        else:
            return []

    def search_resources_by_name(self, name: str, as_frame: bool = True):
        """
        Retrieves a dataset by its name.

        :param name: Name of the dataset to retrieve
        :type name: str
        :param as_frame: Whether to return the result as a Pandas DataFrame, defaults to True
        :type as_frame: bool, optional
        :return: Dataset corresponding to the given name
        :rtype: list or pd.DataFrame or None
        """
        try:
            result = self.ckan.action.package_show(id=name)['resources']
        except CKANAPIError:
            return None

        if as_frame:
            return pd.DataFrame(result)
        else:
            return result

    def download_dataset(self, name: str, file_path: str = './cache/', overwrite: bool = False):
        """
        Downloads a dataset.

        :param name: Name of the dataset to download
        :type name: str
        :param file_path: Directory where the downloaded files will be stored, defaults to './cache/'
        :type file_path: str, optional
        :param overwrite: Whether to overwrite existing files, defaults to False
        :type overwrite: bool, optional
        :return: None
        """
        file_path = Path(file_path) / name
        file_path.mkdir(parents=True, exist_ok=True)

        resources = self.search_resources_by_name(name, as_frame=False)

        downloaded_resources = []

        for resource in tqdm(resources):
            if resource['url_type'] == 'upload':
                download_path = file_path / resource['name']
                if not download_path.exists() or overwrite:
                    wget.download(resource['url'], out=str(file_path / resource['name']))
                    downloaded_resources.append(resource['name'])
                else:
                    print(f'File {download_path} already exists. Skipping...')

        print(f'\nDownloaded {len(downloaded_resources)} resources: {downloaded_resources}')

    def load(self,
             name: str,
             filename: str = None,
             file_path: str = './cache/',
             reload: bool = False,
             smart_return: bool = True
             ):
        """
        Loads a file from a specified dataset.

        :param name: Name of the dataset to load from
        :type name: str
        :param filename: Name of the file to load, defaults to None
        :type filename: str, optional
        :param file_path: Directory where the file will be saved or loaded from, defaults to './cache/'
        :type file_path: str, optional
        :param reload: Whether to download the file again even if it exists, defaults to False
        :type reload: bool, optional
        :param smart_return: Whether to attempt returning a loaded object instead of a file path, defaults to True
        :type smart_return: bool, optional
        :return: File path or loaded object
        :rtype: str or object
        """

        # First, search for the name and make sure it exists
        dataset = self.search_resources_by_name(name, as_frame=True)
        if dataset is None:
            raise ValueError(f'Dataset {name} not found')

        # Second, if the file is not specified, print out a list of the options
        if filename is None:
            raise ValueError(f'Please specify a file name from the following options:\n{dataset["name"].values}')

        # Third, if the file is specified, make sure it exists
        if filename not in dataset['name'].values:
            raise ValueError(f'File {filename} not found in dataset {name} with options:\n{dataset["name"].values}')

        # Fourth, verify that the resource has a non-nan value in the url column
        url = dataset[dataset['name'] == filename]['url'].values[0]
        if pd.isna(url):
            raise ValueError(f'File {filename} in dataset {name} does not have a valid url')

        if smart_return:
            # Attempt to discover the file type
            file_type = dataset[dataset['name'] == filename]['format'].values[0].lower()
            if file_type in self.smart_return_filetypes:
                return getattr(self, f'load_{file_type}')(name, filename, file_path=file_path, reload=reload)

        # Now download the file
        file_path = Path(file_path) / name
        file_path.mkdir(parents=True, exist_ok=True)

        file_path = file_path / filename
        if not file_path.exists() or reload:
            #print(f'Downloading {filename}...')
            wget.download(url, out=str(file_path))
            #print(f'\nDownloaded {filename} to {file_path}')

        return file_path

    def load_csv(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if Pandas is installed
        try:
            import pandas as pd
            return pd.read_csv(file_path)
        except ImportError:
            # Read the file with the csv module

            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                data = list(reader)
            return data

    def load_docx(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if docx is installed
        try:
            import docx
            doc = docx.Document(file_path)
            return doc
        except ImportError:
            # Can't read docx file
            raise ImportError('Please install python-docx to read docx files.')

    def load_gpkg(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if geopandas is installed
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            # Can't read geopackage file
            raise ImportError('Please install geopandas to read geopackage files.')

    def load_geojson(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if geopandas is installed
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            # Can't read geojson file
            raise ImportError('Please install geopandas to read geojson files.')

    def load_jpeg(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if PIL, Matplotlib, NumPy, or OpenCV is installed
        try:
            import PIL
            image = PIL.Image.open(file_path)
            return image
        except ImportError:
            pass

        try:
            import matplotlib.pyplot as plt
            image = plt.imread(file_path)
            return image
        except ImportError:
            pass

        try:
            import numpy as np
            image = np.load(file_path)
            return image
        except ImportError:
            pass

        try:
            import cv2
            image = cv2.imread(file_path)
            return image
        except ImportError:
            # Can't read jpeg file
            raise ImportError('Please install PIL, Matplotlib, NumPy, or OpenCV to read jpeg files.')

    def load_json(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        import json
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data

    def load_kml(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if geopandas is installed
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            # Can't read kml file
            raise ImportError('Please install geopandas to read kml files.')

    def load_pdf(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if PyPDF2 is installed
        try:
            import PyPDF2
            pdf = PyPDF2.PdfFileReader(file_path)
            return pdf
        except ImportError:
            # Can't read pdf file
            raise ImportError('Please install PyPDF2 to read pdf files.')

    def load_sav(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if savReaderWriter is installed
        try:
            from savReaderWriter import SavReader
            with SavReader(file_path) as reader:
                data = reader.all()
            return data
        except ImportError:
            # Can't read sav file
            raise ImportError('Please install savReaderWriter to read sav files.')

    def load_shp(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if geopandas is installed
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            # Can't read shp file
            raise ImportError('Please install geopandas to read shp files.')

    def load_txt(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        with open(file_path, 'r') as f:
            data = f.read()
        return data

    def load_xlsm(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if Pandas is installed
        try:
            import pandas as pd
            return pd.read_excel(file_path, engine='openpyxl')
        except ImportError:
            # Can't read xlsm file
            raise ImportError('Please install pandas with openpyxl to read xlsm files.')

    def load_xlsx(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if Pandas is installed
        try:
            import pandas as pd
            return pd.read_excel(file_path)
        except ImportError:
            # Can't read xlsx file
            raise ImportError('Please install pandas with openpyxl to read xlsx files.')

    def load_xml(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if lxml is installed
        try:
            from lxml import etree
            tree = etree.parse(file_path)
            return tree
        except ImportError:
            # Can't read xml file
            raise ImportError('Please install lxml to read xml files.')

    def load_xsd(self, name, filename, **kwargs):
        file_path = self.load(name, filename, smart_return=False, **kwargs)
        # Check if lxml is installed
        try:
            from lxml import etree
            tree = etree.parse(file_path)
            return tree
        except ImportError:
            # Can't read xsd file
            raise ImportError('Please install lxml to read xsd files.')
