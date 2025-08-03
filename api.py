"""
API module for interacting with Toronto Open Data CKAN portal.
"""

from typing import List, Dict, Any, Optional, Union
import pandas as pd
from ckanapi import RemoteCKAN
from ckanapi.errors import CKANAPIError

try:
    from .config import config
except ImportError:
    from config import config


class TorontoOpenDataAPI:
    """Handles all API interactions with Toronto Open Data portal."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the API client.
        
        Args:
            api_key: Optional API key for accessing the Toronto Open Data portal
        """
        self.ckan = RemoteCKAN(config.API_BASE_URL, apikey=api_key)
    
    def list_all_datasets(self, as_frame: bool = True) -> Union[List[str], pd.DataFrame]:
        """
        List all available datasets.
        
        Args:
            as_frame: Whether to return the result as a Pandas DataFrame
            
        Returns:
            List of datasets as DataFrame or list
        """
        result = self.ckan.action.package_list()
        
        if as_frame:
            return pd.DataFrame(result)
        return result
    
    def search_datasets(self, query: str, as_frame: bool = True) -> Union[List[Dict], pd.DataFrame]:
        """
        Search datasets by keyword.
        
        Args:
            query: Keyword to search for
            as_frame: Whether to return the result as a Pandas DataFrame
            
        Returns:
            List of datasets that match the query
        """
        result = self.ckan.action.package_search(q=query)
        
        if 'results' in result:
            if as_frame:
                return pd.DataFrame(result['results'])
            return result['results']
        return []
    
    def get_dataset_resources(self, name: str, as_frame: bool = True) -> Optional[Union[List[Dict], pd.DataFrame]]:
        """
        Get resources for a specific dataset.
        
        Args:
            name: Name of the dataset to retrieve
            as_frame: Whether to return the result as a Pandas DataFrame
            
        Returns:
            Dataset resources as DataFrame or list, or None if not found
        """
        try:
            result = self.ckan.action.package_show(id=name)['resources']
        except CKANAPIError:
            return None
        
        if as_frame:
            return pd.DataFrame(result)
        return result
    
    def get_dataset_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get complete dataset information.
        
        Args:
            name: Name of the dataset to retrieve
            
        Returns:
            Complete dataset information or None if not found
        """
        try:
            return self.ckan.action.package_show(id=name)
        except CKANAPIError:
            return None 