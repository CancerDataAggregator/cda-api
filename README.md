# cda-api

## About
The Application Programming Interface (API) for the Cancer Data Aggregator project. This tool allows users to search for National Cancer Institute (NCI) data by leveraging [cdapython](https://github.com/CancerDataAggregator/cdapython) or directly through the Swagger/Redoc pages. 

The API is built in Python leveraging the FastAPI module. FastAPI auto generates an OpenAPI schema which is used to automatically generate the [cda-client](https://github.com/CancerDataAggregator/cda-client) library that allows for [cdapython](https://github.com/CancerDataAggregator/cdapython) to make calls against the deployed API. 

## Endpoint Descriptions
### /data/file
Returns list of json objects containing information about files based on the filters provided in the request
body.

### /data/subject
Returns list of json objects containing information about subjects based on the filters provided in the request
body.

### /summary/file
Returns a json object containing summarizations of file data based on the filters provided in the request
body.

### /summary/subject
Returns a json object containing summarizations of subject data based on the filters provided in the request
body.

### /columns
Returns a list json objects containing information on all queryable columns within the data.

### /unique_values
Returns a json object containing information about the provided column such as the unique values present
in the data for that column, the number of times each unique value appears in the data, etc.

### /release_metadata
Returns a list of json objects containing information about the current release of data within CDA.

## Example API calls
### cdapython example
```python
from cdapython import get_file_data
get_file_data(match_all=["size < 100", "format = T*"])
```

### /data/file endpoint request body Swagger page example
Navigate to: https://cda.datacommons.cancer.gov/docs#/data/file_fetch_rows_endpoint_data_file_post

Click on the 'Try it out' button and enter the following into the Request body field
```json
{
  "MATCH_ALL": ["size < 100", "format like T*"],
  "MATCH_SOME": [],
  "ADD_COLUMNS": [],
  "EXCLUDE_COLUMNS": [],
  "COLLATE_RESULTS": false,
  "EXTERNAL_REFERENCE": false
}
```

### Curl example:
```sh
curl -X 'POST' \
  'https://cda.datacommons.cancer.gov/data/file?limit=100&offset=0' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "MATCH_ALL": ["size < 100", "format like T*"],
  "MATCH_SOME": [],
  "ADD_COLUMNS": [],
  "EXCLUDE_COLUMNS": [],
  "COLLATE_RESULTS": false,
  "EXTERNAL_REFERENCE": false
}'
```

## Links

- [Production API Swagger Page](https://cda.datacommons.cancer.gov/docs)
- [Production API Redoc Page](https://cda.datacommons.cancer.gov/redoc)
- API URL for developing against: https://cda.datacommons.cancer.gov/
- [cdapython GitHub Page](https://github.com/CancerDataAggregator/cdapython)
- [cda-client GitHub Page](https://github.com/CancerDataAggregator/cda-client)
- [CDA-HelpDesk GitHub Page](https://github.com/CancerDataAggregator/CDA-HelpDesk)
- [FastAPI](https://fastapi.tiangolo.com/)