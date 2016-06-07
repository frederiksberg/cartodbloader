# CartoDB Loader
This is a script to load a geojson file into CartoDB through the SQL API, as
the otherwise magnificent `ogr2ogr` seems to mess up the URL encodes. At the
moment, it only does updates, the initial load should still be done with
`ogr2ogr`to get the data types right.

Depends on shapely and some python standard libraries
(sys, json, urllib, urllib2, and optparse).

## Warning!
This script has NOT been extensively tested. If it does something horrible
to your data, computer, or the internet as a whole, it is not my fault.
It is probably not very memory efficient, and should not be used with large
datasets.

## Usage
```
Usage: cartodbloader.py [options] target_table source_file

Options:
  -h, --help            show this help message and exit
  -k APIKEY, --apikey=APIKEY
                        API key for CartoDB
  -a ACCOUNT, --account=ACCOUNT
                        CartoDB account name
  -u URL, --url=URL     Custom URL endpoint
  -c CHUNK_SIZE, --chunk-size=CHUNK_SIZE
                        Chunk size of bulk inserts
```
(currently, our account name (frederiksberg) is set as the default if no account or url is provided)

### Example
```cmd
python cartodbloader.py --account=myaccount --apikey=0123456789abcdeffedcba987654321001234567  a_table_in_cartodb my_local_data.geojson
```
