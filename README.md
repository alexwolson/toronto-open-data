# TorontoOpenData Python Package

## Overview

The `TorontoOpenData` package provides a Python interface to interact with the Toronto Open Data portal. It allows users to list, search, and download datasets, as well as load specific resources.

## Installation

To install the package, run:

```bash
pip install toronto-open-data
```

## Dependencies

- `pandas`
- `requests`
- `tqdm`
- `ckanapi`

## Usage

### Initialization

Initialize the `TorontoOpenData` class:

```python
from toronto_open_data import TorontoOpenData

tod = TorontoOpenData(api_key='your_api_key_here')  # API key is optional
```

### List All Datasets

List all available datasets:

```python
datasets = tod.list_all_datasets()
```

### Search Datasets

Search datasets by keyword:

```python
search_results = tod.search_datasets('parks')
```

### Download Dataset

Download a specific dataset:

```python
tod.download_dataset('dataset_name')
```

### Load Dataset

Load a specific file from a dataset:

```python
file_path = tod.load('dataset_name', 'file_name.csv', smart_return=False)
```

Load a specific file, returning an object if supported (default behaviour):

```python
file_object = tod.load('dataset_name', 'file_name.csv', smart_return=True)
```

## Methods

- `list_all_datasets(as_frame=True)`: List all datasets.
- `search_datasets(query, as_frame=True)`: Search datasets by keyword.
- `search_resources_by_name(name, as_frame=True)`: Get dataset by name.
- `download_dataset(name, file_path='./cache/', overwrite=False)`: Download resource.
- `load(name, filename, file_path='./cache/', reload=False, smart_return=True)`: Load a file from the dataset.

## Smart Return File Types

The package supports smart return for the following file types:

- csv
- docx
- gpkg
- geojson
- jpeg
- json
- kml
- pdf
- sav
- shp
- txt
- xlsm
- xlsx
- xml
- xsd

## License

MIT License
