"""
Configuration settings for the Toronto Open Data package.
"""

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration settings for Toronto Open Data package."""
    
    # API Configuration
    API_BASE_URL: str = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    
    # Cache Configuration
    DEFAULT_CACHE_PATH: str = "./cache/"
    
    # Supported file types for smart return
    SMART_RETURN_FILETYPES: List[str] = None
    
    # File type to loader mapping
    FILE_TYPE_LOADERS: Dict[str, str] = None
    
    def __post_init__(self):
        if self.SMART_RETURN_FILETYPES is None:
            self.SMART_RETURN_FILETYPES = [
                'csv', 'docx', 'gpkg', 'geojson', 'jpeg', 'json', 'kml',
                'pdf', 'sav', 'shp', 'txt', 'xlsm', 'xlsx', 'xml', 'xsd'
            ]
        
        if self.FILE_TYPE_LOADERS is None:
            self.FILE_TYPE_LOADERS = {
                'csv': 'load_csv',
                'docx': 'load_docx',
                'gpkg': 'load_gpkg',
                'geojson': 'load_geojson',
                'jpeg': 'load_jpeg',
                'json': 'load_json',
                'kml': 'load_kml',
                'pdf': 'load_pdf',
                'sav': 'load_sav',
                'shp': 'load_shp',
                'txt': 'load_txt',
                'xlsm': 'load_xlsm',
                'xlsx': 'load_xlsx',
                'xml': 'load_xml',
                'xsd': 'load_xsd'
            }


# Global configuration instance
config = Config() 