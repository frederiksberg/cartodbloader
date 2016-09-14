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

import json, urllib, sys, argparse
from shapely import wkb, geometry
from urllib2 import Request, urlopen, URLError
from optparse import OptionParser

def feature_to_values(feature, properties, expected_geom_type, promote_to_multi):
    """This function takes a feature and converts it to a list of values to be
    inserted by a PostgreSQL insert statement. It is in the order dictated by
    the properties list, followed by the geometry and then by the id.
    """
    # Convert the geomertry to a wkb using shapely
    geom = geometry.shape(feature['geometry'])
    geom_type = feature['geometry']['type']
    if not expected_geom_type == 'GEOMETRY' \
        and not geom_type.upper() == expected_geom_type:
            geom = promote_to_multi([geom])
    feat_wkb = geom.wkb.encode('hex')

    # Convert feature properties to a list of values to be used
    # in constructing a SQL insert. Strings have single-quotes added and None are
    # converted to NULL
    values = []
    for p in properties:
        value = feature['properties'][p]
        if value.__class__ == unicode:
            values.append(u"'{0}'".format(value.replace('\'','\'\'')))
        elif value.__class__ == None.__class__:
            values.append(u'NULL')
        else:
            values.append(u'{0}'.format(value))

    # Append geometry to value list. Because of Mapnik issues with vertice
    # direction, force all polygons to be in right hand direction.
    if geom_type == "MultiPolygon" or geom_type == 'Polygon':
        values.append(u"ST_ForceRHR(ST_GeomFromWKB(E'\\\\x{0}',4326))".format(feat_wkb))
    else:
        values.append(u"ST_GeomFromWKB(E'\\\\x{0}',4326)".format(feat_wkb))

    # Add default next id.
    values.append(u"nextval('{0}_cartodb_id_seq')".format(cartodb_layer))

    # return value list
    return(u'(' + u','.join(values) + u')')

def run_query(q, cartodb_api_key=None):
    """Function to run a query on the CartoDB SQL API"""
    # Do the encodes
    data = urllib.urlencode({'q':q.encode('utf8'),'api_key': cartodb_api_key,})
    # Construct the request. Because insert statements can get quite long, use
    # a POST request.
    request = Request(cartodb_api_url,data)
    try:
        # Send the request and try tro return the data from the response
        response = urlopen(request)
        return(json.loads(response.read()))
    except URLError, e:
        # On an error, write it to stderr
        # TODO: Figure out how to get the error data in the output
        sys.stderr.write('Error: {0}\n'.format(e))
        sys.stderr.write('Data: {0}\n'.format(data))
        sys.stderr.write('----------------\n')
        return(False)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def main(cartodb_api_url, cartodb_layer, cartodb_api_key, data):
    # Check target geometry type. If not found, it is missing.
    # TODO: It would probably be a good idea to check if it has the expected attribute fields
    q = u"SELECT type FROM geometry_columns WHERE f_table_name = '{0}' AND f_geometry_column = 'the_geom'".format(cartodb_layer)
    cdb_geom_result = run_query(q, cartodb_api_key)
    if not cdb_geom_result['rows']:
        print("Table does not seem to exist in CartoDB. Right now, this script only does updates. Now exiting.")
        sys.exit()
    else:
        cdb_geom_type = cdb_geom_result['rows'][0]['type']

    # Inform the user
    print("Target geometry type is {0}".format(cdb_geom_type))

    # Determine which functions should be used to promote non-multi geometries
    # to multi, if they should appear in the data
    if cdb_geom_type == 'MULTIPOLYGON':
        promote_to_multi = geometry.multipolygon.asMultiPolygon
    elif cdb_geom_type == 'MULTILINESTRING':
        promote_to_multi = geometry.multilinestring.asMultiLineString
    elif cdb_geom_type == 'MULTIPOINT':
        promote_to_multi = geometry.multipoint.asMultiPoint
    else:
        promote_to_multi = None

    # Clean up existing table and restart the sequence.
    print("Clearing existing data")
    run_query('TRUNCATE {0}'.format(cartodb_layer))
    run_query('ALTER SEQUENCE {0}_cartodb_id_seq RESTART WITH 1'.format(cartodb_layer))

    # Get a list of attributes from the first feature (assumes that all features
    # have the same attributes).
    properties = data['features'][0]['properties'].keys()

    # Tell the user that something is happening
    print('Loading {0} pieces of data'.format(len(data['features'])))

    # Run through the features and insert them.
    # TODO: Maybe the requests could be done asynchronously
    for chunk in chunks(data['features'],options.chunk_size):
        # Construct a list of fields to be inserted (add the_geom and cartodb_id)
        # ogr2ogr does not remove spaces, but still lowercases the field names
        # Only ascii characters ar lowercased by ogr2ogr, so unicode needs to be
        # encoded before lowercasing.
        field_list = ['"' + s.encode('utf8').lower().decode('utf8') + '"' for s in properties+[u'the_geom',u'cartodb_id']]
        fields = u','.join(field_list)

        value_parts = []
        for feature in chunk:
            # Check if geometry is present. We don't want nonspatial data in cartoDB
            if feature['geometry']:
                value_part = feature_to_values(feature, properties,
                                               cdb_geom_type, promote_to_multi)
                # Add value list to the list of value lists :-)
                value_parts.append(value_part)

        # Construct the insert statement and run it.
        q = u"INSERT INTO {0} ({1}) VALUES {2}".format(cartodb_layer, fields, u",".join(value_parts))
        run_query(q)

    # Run vacuum on the table
    print("Vacuuming table")
    run_query(u"VACUUM {0}".format(cartodb_layer))

    print "Done"

if __name__ == "__main__":
    # ArgParse setup
    parser = argparse.ArgumentParser(description='Load a geojson file into Carto through the SQL API.')

    # Positional arguments
    parser.add_argument('cartodb_layer', metavar='target_table', help = "Target table in Carto")
    parser.add_argument('input_file', metavar='source_file', help = "Target table in Carto")

    # Api-key. As this script mostly does writes, it is required
    parser.add_argument('-k', '--apikey', dest='cartodb_api_key', help="API key for Carto", required=True)

    # Connection to Carto. account and URL are mutually exclusive
    url_group = parser.add_mutually_exclusive_group(required=True)
    url_group.add_argument("-a", "--account", dest="account", default="frederiksberg", help="CartoDB account name")
    url_group.add_argument("-u", "--url", dest="url", help="Custom URL endpoint")

    # An advanced option. You should probably not fiddle with this.
    parser.add_argument("-c", "--chunk-size", type=int, dest="chunk_size", default=50, help="Chunk size of bulk inserts")

    # Parse the arguments
    args = parser.parse_args()
    # If a custom url is provided, use it. Alternatively, construct one from account

    if args.url:
        cartodb_api_url = url
    else:
        cartodb_api_url = u'https://{0}.cartodb.com/api/v2/sql'.format(args.account)

    # Get data from input file
    with open(args.input_file,'r') as f:
        data = json.load(f)

    # Actually do something
    main(cartodb_api_url, args.cartodb_layer, args.cartodb_api_key, data)
