Script to load a geojson file into CartoDB through the SQL API, as OGR seems
to mess up the urlencodes. At the moment, it only does updates, the initial load
should still be done with ogr to get the datatypes right.

Depends on shapely and some python standard libraries
(json, urllib, urllib2, and optparse).

It is propbably not very memory efficient, and should not be used with large
datasets. It is also currently dreadfully slow
