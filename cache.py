"""
Cache module for handling file downloads and caching.
"""

from typing import List
from pathlib import Path
import requests

try:
    from .config import config
except ImportError:
    from config import config


class FileCache:
    """Handles file downloading and caching operations."""
    
    def __init__(self, cache_path: str = None):
        """
        Initialize the file cache.
        
        Args:
            cache_path: Directory where downloaded files will be stored
        """
        self.cache_path = Path(cache_path or config.DEFAULT_CACHE_PATH)
        self.cache_path.mkdir(parents=True, exist_ok=True)
    
    def download_dataset(self, 
                        name: str, 
                        resources: List[dict], 
                        overwrite: bool = False) -> List[str]:
        """
        Download all resources for a dataset.
        
        Args:
            name: Name of the dataset
            resources: List of resource dictionaries
            overwrite: Whether to overwrite existing files
            
        Returns:
            List of downloaded resource names
        """
        dataset_path = self.cache_path / name
        dataset_path.mkdir(parents=True, exist_ok=True)
        
        downloaded_resources = []
        
        for resource in resources:
            if resource['url_type'] == 'upload':
                download_path = dataset_path / resource['name']
                
                if not download_path.exists() or overwrite:
                    try:
                        response = requests.get(resource['url'], stream=True)
                        response.raise_for_status()
                        with open(download_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        downloaded_resources.append(resource['name'])
                    except Exception as e:
                        print(f"Failed to download {resource['name']}: {e}")
                else:
                    print(f'File {download_path} already exists. Skipping...')
        
        print(f'\nDownloaded {len(downloaded_resources)} resources: {downloaded_resources}')
        return downloaded_resources
    
    def download_file(self, 
                     dataset_name: str, 
                     filename: str, 
                     url: str, 
                     reload: bool = False) -> Path:
        """
        Download a single file.
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file to download
            url: URL to download from
            reload: Whether to download again even if file exists
            
        Returns:
            Path to the downloaded file
        """
        dataset_path = self.cache_path / dataset_name
        dataset_path.mkdir(parents=True, exist_ok=True)
        
        file_path = dataset_path / filename
        
        if not file_path.exists() or reload:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        return file_path
    
    def get_file_path(self, dataset_name: str, filename: str) -> Path:
        """
        Get the expected file path for a dataset and filename.
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            Expected file path
        """
        return self.cache_path / dataset_name / filename
    
    def file_exists(self, dataset_name: str, filename: str) -> bool:
        """
        Check if a file exists in cache.
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            True if file exists, False otherwise
        """
        file_path = self.get_file_path(dataset_name, filename)
        return file_path.exists() 