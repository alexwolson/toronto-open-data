"""
File loaders module for handling different file types.
"""

from typing import Any, Dict, Callable
from pathlib import Path
import csv
import json
import pandas as pd

try:
    from .config import config
except ImportError:
    from config import config


class FileLoaderFactory:
    """Factory for creating file loaders based on file type."""
    
    def __init__(self):
        """Initialize the loader factory with all supported loaders."""
        self._loaders: Dict[str, Callable] = {
            'csv': self._load_csv,
            'json': self._load_json,
            'txt': self._load_txt,
            'docx': self._load_docx,
            'gpkg': self._load_gpkg,
            'geojson': self._load_geojson,
            'jpeg': self._load_jpeg,
            'kml': self._load_kml,
            'pdf': self._load_pdf,
            'sav': self._load_sav,
            'shp': self._load_shp,
            'xlsm': self._load_xlsm,
            'xlsx': self._load_xlsx,
            'xml': self._load_xml,
            'xsd': self._load_xsd
        }
    
    def get_loader(self, file_type: str) -> Callable:
        """
        Get the appropriate loader for a file type.
        
        Args:
            file_type: The file type/extension
            
        Returns:
            Loader function for the file type
            
        Raises:
            ValueError: If file type is not supported
        """
        file_type = file_type.lower()
        if file_type not in self._loaders:
            raise ValueError(f"Unsupported file type: {file_type}")
        return self._loaders[file_type]
    
    def load_file(self, file_path: Path, file_type: str) -> Any:
        """
        Load a file using the appropriate loader.
        
        Args:
            file_path: Path to the file to load
            file_type: Type of the file
            
        Returns:
            Loaded file content
        """
        loader = self.get_loader(file_type)
        return loader(file_path)
    
    def _load_csv(self, file_path: Path) -> Any:
        """Load CSV file."""
        try:
            return pd.read_csv(file_path)
        except ImportError:
            # Fallback to csv module
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                return list(reader)
    
    def _load_json(self, file_path: Path) -> Any:
        """Load JSON file."""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def _load_txt(self, file_path: Path) -> str:
        """Load text file."""
        with open(file_path, 'r') as f:
            return f.read()
    
    def _load_docx(self, file_path: Path) -> Any:
        """Load DOCX file."""
        try:
            import docx
            return docx.Document(file_path)
        except ImportError:
            raise ImportError('Please install python-docx to read docx files.')
    
    def _load_gpkg(self, file_path: Path) -> Any:
        """Load GeoPackage file."""
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            raise ImportError('Please install geopandas to read geopackage files.')
    
    def _load_geojson(self, file_path: Path) -> Any:
        """Load GeoJSON file."""
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            raise ImportError('Please install geopandas to read geojson files.')
    
    def _load_jpeg(self, file_path: Path) -> Any:
        """Load JPEG file."""
        # Try PIL first
        try:
            import PIL
            return PIL.Image.open(file_path)
        except ImportError:
            pass
        
        # Try matplotlib
        try:
            import matplotlib.pyplot as plt
            return plt.imread(file_path)
        except ImportError:
            pass
        
        # Try numpy
        try:
            import numpy as np
            return np.load(file_path)
        except ImportError:
            pass
        
        # Try OpenCV
        try:
            import cv2
            return cv2.imread(str(file_path))
        except ImportError:
            pass
        
        raise ImportError('Please install PIL, Matplotlib, NumPy, or OpenCV to read jpeg files.')
    
    def _load_kml(self, file_path: Path) -> Any:
        """Load KML file."""
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            raise ImportError('Please install geopandas to read kml files.')
    
    def _load_pdf(self, file_path: Path) -> Any:
        """Load PDF file."""
        try:
            import PyPDF2
            return PyPDF2.PdfFileReader(file_path)
        except ImportError:
            raise ImportError('Please install PyPDF2 to read pdf files.')
    
    def _load_sav(self, file_path: Path) -> Any:
        """Load SAV file."""
        try:
            from savReaderWriter import SavReader
            with SavReader(file_path) as reader:
                return reader.all()
        except ImportError:
            raise ImportError('Please install savReaderWriter to read sav files.')
    
    def _load_shp(self, file_path: Path) -> Any:
        """Load Shapefile."""
        try:
            import geopandas as gpd
            return gpd.read_file(file_path)
        except ImportError:
            raise ImportError('Please install geopandas to read shp files.')
    
    def _load_xlsm(self, file_path: Path) -> Any:
        """Load XLSM file."""
        try:
            return pd.read_excel(file_path, engine='openpyxl')
        except ImportError:
            raise ImportError('Please install pandas with openpyxl to read xlsm files.')
    
    def _load_xlsx(self, file_path: Path) -> Any:
        """Load XLSX file."""
        try:
            return pd.read_excel(file_path)
        except ImportError:
            raise ImportError('Please install pandas with openpyxl to read xlsx files.')
    
    def _load_xml(self, file_path: Path) -> Any:
        """Load XML file."""
        try:
            from lxml import etree
            return etree.parse(file_path)
        except ImportError:
            raise ImportError('Please install lxml to read xml files.')
    
    def _load_xsd(self, file_path: Path) -> Any:
        """Load XSD file."""
        try:
            from lxml import etree
            return etree.parse(file_path)
        except ImportError:
            raise ImportError('Please install lxml to read xsd files.')


# Global loader factory instance
loader_factory = FileLoaderFactory() 