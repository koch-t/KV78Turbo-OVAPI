import uwsgi
import psycopg2
from psycopg2.pool import PersistentConnectionPool
import simplejson
from sphinxapi import *
from urlparse import urlparse,urlsplit,parse_qs
import re

COMMON_HEADERS = [('Content-Type', 'application/json'), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Requested-With,Content-Type')]

pool = PersistentConnectionPool(1,20,"dbname='haltes'")
cl = SphinxClient()
cl.SetServer('localhost', 9312)
cl.SetWeights ( [100, 1] )
cl.SetMatchMode ( SPH_MATCH_EXTENDED )
cl.Open()

#update timingpoint set latitude = CAST(ST_Y(the_geom) AS NUMERIC(9,7)), longitude = CAST(ST_X(the_geom) AS NUMERIC(8,7)) FROM (select ST_Transform(st_setsrid(st_makepoint(locationx_ew, locationy_ns), 28992), 4326) AS the_geom from timingpoint as t2 where t2.timingpointcode = timingpointcode) AS W;   

def notfound(start_response):
    start_response('404 File Not Found', COMMON_HEADERS + [('Content-length', '2')])
    yield '[]'

def searchStops(query):
    reply = {'Columns' : ['TimingPointTown','TimingPointName', 'Name', 'Latitude', 'Longitude'] , 'Rows' : []}
    if query == 'search_suggest_query' or query == None or query == '' or len(query) < 2:
       return reply
    cl.Open()
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
        cl.Close()
        cl.Open()
	pass
    return reply
            
	
def queryTowns(conn,environ, start_response):
    reply = {'Columns' : ['TimingPointTown'], 'Rows' : []}
    conn = pool.getconn()
    cur = conn.cursor()
    cur.execute("SELECT distinct timingpointtown FROM timingpoint ORDER BY timingpointtown", [])
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0]])
    cur.close()
    return reply

def queryStops(conn,environ, start_response):
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
            geomconstant = 'SRID=4326;POINT('+longitude+' '+latitude+')'
    	    cur.execute("SELECT timingpointtown,name,latitude,longitude,stopareacode FROM (SELECT distinct on (timingpointtown,name) timingpointtown,name,latitude,longitude,stopareacode,distance FROM (select timingpointtown,name,latitude,longitude,stopareacode, ST_Distance(the_geom, st_setsrid(st_makepoint(%s, %s),4326)) as distance from timingpoint ORDER by the_geom <-> %s::geometry LIMIT %s) as x) as y order by distance;", [longitude,latitude,geomconstant,limit])
    elif 'search' in params:
            return searchStops(params['search'][0])
    else:
    	    return '404'
    rows = cur.fetchall()
    for row in rows:
    	    if len(reply['Columns']) == 5:
    	       reply['Rows'].append([row[0],row[1],row[2],row[3],row[4]])
    	    if len(reply['Columns']) == 6:
    	       reply['Rows'].append([row[0],row[1],row[2],row[3],row[4],row[5]])
    cur.close()
    return reply

def queryStopAreas(conn,environ, start_response):
    params = parse_qs(environ.get('QUERY_STRING',''))
    reply = {'Columns' : ['TimingPointTown', 'Name', 'Latitude', 'Longitude', 'StopAreaCode'] , 'Rows' : []}
    cur = conn.cursor()
    if 'near' in params:
    	    latitude, longitude = params['near'][0].split(',')
    	    limit = '100'
    	    if 'limit' in params:
    	      	limit = params['limit'][0]
	    limit5 = int(limit)*5
    	    cur = conn.cursor()
            geomconstant = 'SRID=4326;POINT('+longitude+' '+latitude+')'
    	    cur.execute("SELECT timingpointtown,name,latitude,longitude,stopareacode FROM (select distinct on (stopareacode) * from (select *, ST_Distance(the_geom, st_setsrid(st_makepoint(%s, %s),4326)) as distance from timingpoint ORDER by the_geom <-> %s::geometry LIMIT %s) as x) as y order by distance LIMIT %s;", [latitude,longitude,geomconstant,limit5,limit]) 
    else:
    	    return '404'
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0],row[1],row[2],row[3],row[4]])
    cur.close()
    return reply

def queryAccessibility(conn,environ, start_response):
    params = parse_qs(environ.get('QUERY_STRING',''))
    reply = {'Columns' : ['TimingPointTown', 'TimingPointName', 'Name', 'TimingPointCode', 'kv78turbo','TimingPointWheelChairAccessible', 'TimingPointVisualAccessible', 'Steps','Latitude', 'Longitude'] , 'Rows' : []}
    cur = conn.cursor()
    if 'tpc' in params:
       	    cur.execute("SELECT timingpointtown, timingpointname,name,t.timingpointcode,kv78turbo,motorisch,visueel,trap,t.latitude,t.longitude FROM timingpoint as t left join haltescan as h on (t.timingpointcode = h.timingpointcode) WHERE t.timingpointcode = %s", [params['tpc'][0]])
    else:
            return '404'
    rows = cur.fetchall()
    for row in rows:
    	    reply['Rows'].append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]])
    cur.close()
    return reply

def queryTimingPoints(conn,environ, start_response):
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
    	    limit = '150'
    	    if 'limit' in params:
    	      	limit = params['limit'][0]
            geomconstant = 'SRID=4326;POINT('+longitude+' '+latitude+')'
    	    cur = conn.cursor()
   	    if 'destinations' in params and 'accessibility' in params:
    	    	 reply['Columns'].extend(['DestinationName50','LinePlanningNumber','LinePublicNumber','TimingPointWheelChairAccessible','TimingPointVisualAccessible','Latitude','Longitude'])
                 cur.execute("SELECT timingpointtown, timingpointname, name,x.timingpointcode,stopareacode,kv55,kv78turbo,arriva55,destinationname50,lineplanningnumber,linepublicnumber,motorisch,visueel,x.latitude,x.longitude from  (SELECT latitude,longitude, timingpointtown,timingpointname,name,t.timingpointcode,stopareacode, kv55, kv78turbo, arriva55,destinationname50,lineplanningnumber,linepublicnumber, the_geom FROM (select * from timingpoint ORDER by the_geom <-> %s::geometry LIMIT %s) as t,destinationuserstop as d where d.timingpointcode = t.timingpointcode and destinationcode is not null) as x left join haltescan as h on (h.timingpointcode = x.timingpointcode) ORDER by the_geom <-> %s::geometry LIMIT %s;", [geomconstant,limit,geomconstant,limit])
    	    elif 'destinations' in params:
    	    	 reply['Columns'].extend(['DestinationName50','LinePlanningNumber','LinePublicNumber','Latitude','Longitude'])
    	    	 cur.execute("SELECT timingpointtown,timingpointname,name,t.timingpointcode,stopareacode, kv55, kv78turbo, arriva55,destinationname50,lineplanningnumber,linepublicnumber,latitude,longitude FROM (select * from timingpoint ORDER by the_geom <-> %s::geometry LIMIT %s) as t,destinationuserstop as d where d.timingpointcode = t.timingpointcode and destinationcode is not null ORDER by the_geom <-> %s::geometry LIMIT %s;", [geomconstant,limit,geomconstant,limit])    	    
    	    else:
    	         cur.execute("SELECT timingpointtown,timingpointname,name,timingpointcode,stopareacode, kv55, kv78turbo, arriva55 FROM timingpoint ORDER by the_geom <-> %s::geometry LIMIT %s;", [geomconstant,limit])
    else:
    	    return '404'
    rows = cur.fetchall()
    for row in rows:
            if len(reply['Columns']) == 8:
    	        reply['Rows'].append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]])
    	    elif len(reply['Columns']) == 13:
                reply['Rows'].append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12]])
            elif len(reply['Columns']) == 15:
                reply['Rows'].append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14]])
    cur.close()
    return reply

def HalteDB(environ, start_response):
    url = environ['PATH_INFO'][1:]
    if len(url) > 0 and url[-1] == '/':
	    url = url[:-1]
    arguments = url.split('/')
    reply = None
    conn = pool.getconn()
    if arguments[0] == 'towns':
        reply = queryTowns(conn,environ, start_response)
    elif arguments[0] == 'stops':
    	reply = queryStops(conn,environ, start_response)
    elif arguments[0] == 'stopareas':
        reply = queryStopAreas(conn,environ, start_response)
    elif arguments[0] == 'timingpoints':
    	reply = queryTimingPoints(conn,environ, start_response)
    elif arguments[0] == 'accessibility':
    	reply = queryAccessibility(conn,environ, start_response)
    else:
        pool.putconn(conn)
    	return notfound(start_response)
    pool.putconn(conn)
    if reply == '404':
    	    return notfound(start_response)
    reply = simplejson.dumps(reply)
    start_response('200 OK', COMMON_HEADERS + [('Content-length', str(len(reply)))])
    return reply

uwsgi.applications = {'': HalteDB}
