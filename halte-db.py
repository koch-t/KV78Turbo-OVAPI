import uwsgi
import psycopg2
import simplejson
from sphinxapi import *
from urlparse import urlparse,urlsplit,parse_qs
import re

COMMON_HEADERS = [('Content-Type', 'application/json'), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Requested-With,Content-Type')]

conn = psycopg2.connect("dbname='haltes' user='postgres' password='postgres' port='5433'")
cl = SphinxClient()
cl.SetServer('localhost', 9312)
cl.SetWeights ( [100, 1] )
cl.SetMatchMode ( SPH_MATCH_EXTENDED )


#update timingpoint set latitude = CAST(ST_Y(the_geom) AS NUMERIC(9,7)), longitude = CAST(ST_X(the_geom) AS NUMERIC(8,7)) FROM (select ST_Transform(st_setsrid(st_makepoint(locationx_ew, locationy_ns), 28992), 4326) AS the_geom from timingpoint as t2 where t2.timingpointcode = timingpointcode) AS W;   

def notfound(start_response):
	start_response('404 File Not Found', COMMON_HEADERS + [('Content-length', '2')])
	yield '[]'

def searchStops(query):
    reply = {'Columns' : ['TimingPointTown','TimingPointName', 'Name', 'Latitude', 'Longitude'] , 'Rows' : []}
    res = cl.Query ( query, '*' )
    try:
       if res.has_key('matches'):
         for match in res['matches']:
                row = {}
		for attr in res['attrs']:
			attrname = attr[0]
			value = match['attrs'][attrname]
			row[attrname] = value
		reply['Rows'].append([row['timingpointtown'],row['timingpointname'],row['name'],row['latitude'],row['longitude']])
    except:
	pass
    return reply
            
	
def queryTowns(environ, start_response):
    reply = {'Columns' : ['TimingPointTown'], 'Rows' : []}
    cur = conn.cursor()
    cur.execute("SELECT distinct timingpointtown FROM timingpoint ORDER BY timingpointtown", [])
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0]])
    cur.close()
    return reply

def queryStops(environ, start_response):
    params = parse_qs(environ.get('QUERY_STRING',''))
    reply = {'Columns' : ['TimingPointTown', 'Name', 'Latitude', 'Longitude' , 'StopAreaCode'] , 'Rows' : []}
    cur = conn.cursor()
    if 'town' in params:
       	    cur.execute("SELECT distinct on (timingpointtown,name) timingpointtown,name,latitude,longitude,stopareacode FROM timingpoint WHERE timingpointtown = %s ORDER BY name", [params['town'][0]])
    elif 'tpc' in params:
    	    cur.execute("SELECT distinct on (timingpointtown,name) timingpointtown,name,latitude,longitude,stopareacode FROM timingpoint WHERE timingpointcode = %s ORDER BY name", [params['tpc'][0]])
    elif 'bottomright' in params and 'topleft' in params:
    	    minLatitude, maxLongitude = params['bottomright'][0].split(',')
    	    maxLatitude, minLongitude = params['topleft'][0].split(',')
    	    cur.execute("SELECT distinct on (timingpointtown,name) timingpointtown,name,latitude,longitude,stopareacode FROM timingpoint WHERE latitude > %s AND latitude < %s AND longitude > %s AND longitude < %s", [minLatitude,maxLatitude,minLongitude,maxLongitude])
    elif 'near' in params:
    	    latitude, longitude = params['near'][0].split(',')
    	    limit = '100'
    	    if 'limit' in params:
    	      	limit = params['limit'][0]
    	    cur = conn.cursor()
    	    cur.execute("SELECT timingpointtown,name,latitude,longitude,stopareacode FROM timingpoint  WHERE timingpointcode in (select distinct on (timingpointtown, name)timingpointcode FROM timingpoint) ORDER by ST_Distance(the_geom, st_setsrid(st_makepoint(%s, %s),4326)) LIMIT %s;", [longitude,latitude,limit])
    elif 'search' in params:
            return searchStops(params['search'][0])
    else:
    	    return '404'
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0],row[1],row[2],row[3],row[4]])
    cur.close()
    return reply

def queryStopAreas(environ, start_response):
    params = parse_qs(environ.get('QUERY_STRING',''))
    reply = {'Columns' : ['TimingPointTown', 'Name', 'Latitude', 'Longitude', 'StopAreaCode'] , 'Rows' : []}
    cur = conn.cursor()
    if 'near' in params:
    	    latitude, longitude = params['near'][0].split(',')
    	    limit = '100'
    	    if 'limit' in params:
    	      	limit = params['limit'][0]
    	    cur = conn.cursor()
    	    cur.execute("SELECT timingpointtown,name,latitude,longitude,stopareacode FROM timingpoint  WHERE timingpointcode in (select distinct on (stopareacode)timingpointcode FROM timingpoint) ORDER by ST_Distance(the_geom, st_setsrid(st_makepoint(%s, %s),4326)) LIMIT %s;", [longitude,latitude,limit])
    else:
    	    return '404'
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0],row[1],row[2],row[3],row[4]])
    cur.close()
    return reply
    
def queryTimingPoints(environ, start_response):
    params = parse_qs(environ.get('QUERY_STRING',''))

    reply = {'Columns' : ['TimingPointTown', 'TimingPointName', 'Name', 'TimingPointCode', 'StopAreaCode', 'kv55', 'kv78turbo', 'arriva55'] , 'Rows' : []}
    cur = conn.cursor()
    if 'town' in params and 'timingpointname' in params:
       	    cur.execute("SELECT timingpointtown,timingpointname,name,timingpointcode,stopareacode, kv55, kv78turbo, arriva55 FROM timingpoint WHERE timingpointtown = %s AND timingpoointname = %s", [params['town'][0], params['timingpointname'][0]])
    elif 'town' in params and 'name' in params:
       	    cur.execute("SELECT timingpointtown,timingpointname,name,timingpointcode,stopareacode, kv55, kv78turbo, arriva55 FROM timingpoint WHERE timingpointtown = %s AND name = %s", [params['town'][0], params['name'][0]])
    elif 'timingpointtown' in params:
       	    cur.execute("SELECT timingpointtown,timingpointname,name,timingpointcode,stopareacode, kv55, kv78turbo, arriva55 FROM timingpoint WHERE timingpointtown = %s", [params['town'][0]])
    elif 'tpc' in params:
       	    cur.execute("SELECT timingpointtown,timingpointname,name,timingpointcode,stopareacode, kv55, kv78turbo, arriva55 FROM timingpoint AS t1 WHERE EXISTS (select 1 FROM timingpoint AS t2 WHERE timingpointcode = %s and t1.name = t2.name AND t1.timingpointtown = t2.timingpointtown)", [params['tpc'][0]])
    elif 'near' in params:
    	    latitude, longitude = params['near'][0].split(',')
    	    limit = '100'
    	    if 'limit' in params:
    	      	limit = params['limit'][0]
    	    cur = conn.cursor()
    	    if 'destinations' in params:
    	    	 reply['Columns'].append('DestinationName50')
    	    	 cur.execute("SELECT timingpointtown,timingpointname,name,t.timingpointcode,stopareacode, kv55, kv78turbo, arriva55,destinationname50 FROM timingpoint as t left join destinationuserstop on (t.timingpointcode = destinationuserstop.timingpointcode) ORDER by ST_Distance(the_geom, st_setsrid(st_makepoint(%s, %s),4326)) LIMIT %s;", [longitude,latitude,limit])
    	    else:
    	         cur.execute("SELECT timingpointtown,timingpointname,name,timingpointcode,stopareacode, kv55, kv78turbo, arriva55 FROM timingpoint ORDER by ST_Distance(the_geom, st_setsrid(st_makepoint(%s, %s),4326)) LIMIT %s;", [longitude,latitude,limit])
    else:
    	    return '404'
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]])
    cur.close()
    return reply
	
def HalteDB(environ, start_response):
    url = environ['PATH_INFO'][1:]
    if len(url) > 0 and url[-1] == '/':
	    url = url[:-1]
    arguments = url.split('/')
    reply = None
    if arguments[0] == 'towns':
    	    reply = queryTowns(environ, start_response)
    elif arguments[0] == 'stops':
    	    reply = queryStops(environ, start_response)
    elif arguments[0] == 'stopareas':
            reply = queryStopAreas(environ, start_response)
    elif arguments[0] == 'timingpoints':
    	    reply = queryTimingPoints(environ, start_response)
    else:
    	    return notfound(start_response)
    if reply == '404':
    	    return notfound(start_response)
    
    reply = simplejson.dumps(reply)
    start_response('200 OK', COMMON_HEADERS + [('Content-length', str(len(reply)))])
    return reply

uwsgi.applications = {'': HalteDB}
