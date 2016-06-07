#!/usr/bin/python
# -*- coding: utf8 -*-
# CartoDBLoader.py
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>
#
# Author: Niels Kjøller Hansen <niels.k.h@gmail.com>
# Copyright: Frederiksberg Kommune

"""Script to load a geojson file into CartoDB through the SQL API, as OGR seems
to mess up the urlencodes. At the moment, it only does updates, the initial load
should still be done with ogr to get the datatypes right.

Depends on shapely and some python standard libraries
(json, urllib, urllib2, and optparse).

It is propbably not very memory efficient, and should not be used with large
datasets. It is also currently dreadfully slow
"""

import json, urllib, sys
from shapely import wkb, geometry
from urllib2 import Request, urlopen, URLError
from optparse import OptionParser

# OptParse setup
parser = OptionParser(usage="usage: %prog [options] target_table source_file")
parser.add_option("-k", "--apikey", dest="apikey", help="API key for CartoDB")
parser.add_option("-a", "--account", dest="account", default="frederiksberg", help="CartoDB account name")
parser.add_option("-u", "--url", dest="url", help="Custom URL endpoint")
(options, args) = parser.parse_args()

# Check if required arguments are present
if not args or len(args) < 2:
    parser.error("Not enough arguments")
if not options.apikey:
    parser.error("An API key is required for writing data to CartoDB")
if not (bool(options.url) != bool(options.account)):
    parser.error("You need to provide an account or a custom URL (and not both)")

# Setup basic parameters
input_file = args[1]
cartodb_layer = args[0]
cartodb_api_key = options.apikey

# If a custom url is provided, use it. Alternatively, construct one from account
if options.url:
    cartodb_api_url = url
else:
    cartodb_api_url = u'https://{0}.cartodb.com/api/v2/sql'.format(options.account)

# Get data from input file
with open(input_file,'r') as f:
    data = json.load(f)

def props_to_values(properties,data):
    """Function to convert feature properties to a list of values to be used
    in constructing a SQL insert. Strings have single-quotes added and None are
    converted to NULL"""
    values = []
    for p in properties:
        value = data[p]
        if value.__class__ == unicode:
            values.append(u"'{0}'".format(value.replace('\'','\'\'')))
        elif value.__class__ == None.__class__:
            values.append(u'NULL')
        else:
            values.append(u'{0}'.format(value))
    return values

def run_query(q):
    """Function to run a query on the CartoDB SQL API"""
    # Do the encodes
    data = urllib.urlencode({'q':q.encode('utf8'),'api_key': cartodb_api_key,})
    # Construct the request. Because insert statements can get quite long, use
    # a POST request.
    request = Request(cartodb_api_url,data)
    try:
        # Send the request and try tro return the data from the response
        response = urlopen(request)
        return(response.read())
    except URLError, e:
        # On an error, write it to stderr
        # TODO: Figure out how to get the error data in the output
        sys.stderr.write('Error: {0}\n'.format(e))
        sys.stderr.write('Data: {0}\n'.format(data))
        sys.stderr.write('----------------\n')
        return(False)

# Check if the target table is in CartoDB
# TODO: It would probably be a good idea to check if it has the expected attribute fields
if not run_query(u"SELECT * FROM {0} LIMIT 1".format(cartodb_layer)):
    print("Table does not seem to exist in CartoDB. Right now, this script only does updates. Now exiting.")
    sys.exit()

# Tell the user that something is happening
print('Loading {0} pieces of data'.format(len(data['features'])))


# Clean up existing table and restart the sequence.
run_query('TRUNCATE {0}'.format(cartodb_layer))
run_query('ALTER SEQUENCE {0}_cartodb_id_seq RESTART WITH 1'.format(cartodb_layer))

# Get a list of attributes from the first feature (assumes that all features
# have the same attributes).
properties = data['features'][0]['properties'].keys()

# Run through the features and insert them.
# TODO: Do bulk inserts. Maybe in groups of 5-10 to be safe?
# TODO: Maybe the requests could be done asynchronously
for feature in data['features']:
    # Convert the geomertry to a wkb using shapely
    geom = geometry.shape(feature['geometry'])
    geom_type = feature['geometry']['type']
    feat_wkb = geom.wkb.encode('hex')
    # Construct a list of fields to be inserted (add the_geom and cartodb_id)
    fields = u','.join(properties+['the_geom','cartodb_id'])

    # Construct a list of values to be inserted. Properties from the feature,
    # as well as the geometry and the next id in the sequence.
    values = props_to_values(properties, feature['properties'])

    # Because of Mapnik issues with vertice direction, force all polygons to
    # be in right hand direction.
    if geom_type == "MultiPolygon" or geom_type == 'Polygon':
        values.append(u"ST_ForceRHR(ST_GeomFromWKB(E'\\\\x{0}',4326))".format(feat_wkb))
    else:
        values.append(u"ST_GeomFromWKB(E'\\\\x{0}',4326)".format(feat_wkb))

    values.append(u"nextval('{0}_cartodb_id_seq')".format(cartodb_layer))
    values = u','.join(values)

    # Construct the final insert statement and run it.
    q = u"INSERT INTO {0} ({1}) VALUES({2})".format(cartodb_layer, fields, values)
    run_query(q)

# Run vacuum on the table
run_query(u"UPDATE {0} SET the_geom = ST_ForceRHR(the_geom)".format(cartodb_layer))
run_query(u"VACUUM {0}".format(cartodb_layer))
