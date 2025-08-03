"""
Core module containing the main TorontoOpenData class.
"""

from typing import Optional, Union, Any, List
from pathlib import Path
import pandas as pd

try:
    from .api import TorontoOpenDataAPI
    from .cache import FileCache
    from .loaders import loader_factory
    from .config import config
except ImportError:
    from api import TorontoOpenDataAPI
    from cache import FileCache
    from loaders import loader_factory
    from config import config


class TorontoOpenData:
    """
    Main class for interacting with Toronto Open Data portal.
    
    This class provides a high-level interface for listing, searching,
    downloading, and loading datasets from Toronto's open data portal.
    """
    
    def __init__(self, api_key: Optional[str] = None, cache_path: Optional[str] = None):
        """
        Initialize the Toronto Open Data client.
        
        Args:
            api_key: Optional API key for accessing the Toronto Open Data portal
            cache_path: Directory where downloaded files will be stored
        """
        self.api = TorontoOpenDataAPI(api_key)
        self.cache = FileCache(cache_path)
    
    def list_all_datasets(self, as_frame: bool = True) -> Union[List[str], pd.DataFrame]:
        """
        List all available datasets.
        
        Args:
            as_frame: Whether to return the result as a Pandas DataFrame
            
        Returns:
            List of datasets as DataFrame or list
        """
        return self.api.list_all_datasets(as_frame)
    
    def search_datasets(self, query: str, as_frame: bool = True) -> Union[List[dict], pd.DataFrame]:
        """
        Search datasets by keyword.
        
        Args:
            query: Keyword to search for
            as_frame: Whether to return the result as a Pandas DataFrame
            
        Returns:
            List of datasets that match the query
        """
        return self.api.search_datasets(query, as_frame)
    
    def search_resources_by_name(self, name: str, as_frame: bool = True) -> Optional[Union[List[dict], pd.DataFrame]]:
        """
        Get resources for a specific dataset by name.
        
        Args:
            name: Name of the dataset to retrieve
            as_frame: Whether to return the result as a Pandas DataFrame
            
        Returns:
            Dataset resources as DataFrame or list, or None if not found
        """
        return self.api.get_dataset_resources(name, as_frame)
    
    def download_dataset(self, name: str, overwrite: bool = False) -> List[str]:
        """
        Download all resources for a dataset.
        
        Args:
            name: Name of the dataset to download
            overwrite: Whether to overwrite existing files
            
        Returns:
            List of downloaded resource names
        """
        resources = self.api.get_dataset_resources(name, as_frame=False)
        if resources is None:
            raise ValueError(f'Dataset {name} not found')
        
        return self.cache.download_dataset(name, resources, overwrite)
    
    def load(self,
             name: str,
             filename: Optional[str] = None,
             reload: bool = False,
             smart_return: bool = True) -> Union[Path, Any]:
        """
        Load a file from a specified dataset.
        
        Args:
            name: Name of the dataset to load from
            filename: Name of the file to load
            reload: Whether to download the file again even if it exists
            smart_return: Whether to attempt returning a loaded object instead of a file path
            
        Returns:
            File path or loaded object
            
        Raises:
            ValueError: If dataset or file not found, or file has no valid URL
        """
        # Get dataset resources
        dataset = self.api.get_dataset_resources(name, as_frame=True)
        if dataset is None:
            raise ValueError(f'Dataset {name} not found')
        
        # If filename not specified, show available options
        if filename is None:
            available_files = dataset['name'].values
            raise ValueError(f'Please specify a file name from the following options:\n{available_files}')
        
        # Verify file exists in dataset
        if filename not in dataset['name'].values:
            available_files = dataset['name'].values
            raise ValueError(f'File {filename} not found in dataset {name} with options:\n{available_files}')
        
        # Get file URL and verify it's valid
        file_info = dataset[dataset['name'] == filename].iloc[0]
        url = file_info['url']
        if pd.isna(url):
            raise ValueError(f'File {filename} in dataset {name} does not have a valid url')
        
        # Get file type for smart return
        file_type = file_info['format'].lower()
        
        # Download file if needed
        file_path = self.cache.download_file(name, filename, url, reload)
        
        # Return loaded object or file path
        if smart_return and file_type in config.SMART_RETURN_FILETYPES:
            return loader_factory.load_file(file_path, file_type)
        
        return file_path
    
    def get_dataset_info(self, name: str) -> Optional[dict]:
        """
        Get complete information about a dataset.
        
        Args:
            name: Name of the dataset
            
        Returns:
            Complete dataset information or None if not found
        """
        return self.api.get_dataset_info(name)
    
    def get_available_files(self, name: str) -> List[str]:
        """
        Get list of available files for a dataset.
        
        Args:
            name: Name of the dataset
            
        Returns:
            List of available file names
            
        Raises:
            ValueError: If dataset not found
        """
        dataset = self.api.get_dataset_resources(name, as_frame=True)
        if dataset is None:
            raise ValueError(f'Dataset {name} not found')
        
        return dataset['name'].values.tolist()
    
    def is_file_cached(self, name: str, filename: str) -> bool:
        """
        Check if a file is already cached.
        
        Args:
            name: Name of the dataset
            filename: Name of the file
            
        Returns:
            True if file is cached, False otherwise
        """
        return self.cache.file_exists(name, filename) 