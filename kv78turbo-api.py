import sys
import time
import zmq
from const import ZMQ_KV8, ZMQ_KV78UWSGI, ZMQ_KV7
from ctx import ctx
from datetime import datetime, timedelta
from time import strftime, gmtime
from gzip import GzipFile
from cStringIO import StringIO
import psycopg2
from pymongo import Connection

conn = psycopg2.connect("dbname='kv78turbo' user='postgres' port='5433'")
mongo = Connection()
mongo.drop_database('kv78turbo')
db = mongo.kv78turbo

tpc_store = {}
stopareacode_store = {}
line_store = {}
journey_store = {}
last_updatedataownerstore = {}
generalmessagestore = {}

tpcmeta_store = db.timingpoints
linemeta_store = db.lines
destinationmeta_store = db.destinations
passtimes_store = db.passtimes

cur = conn.cursor()
cur.execute("SELECT dataownercode,lineplanningnumber,linepublicnumber,linename,transporttype from line", [])
rows = cur.fetchall()
for row in rows:
    line_id = row[0] + '_' + row[1]
    line = {'LinePublicNumber' : intern(row[2]), 'LineName' : intern(row[3]), 'TransportType' : intern(row[4]), '_id' : line_id}
    linemeta_store.update({"_id" : line_id},line,True)
cur.close()

cur = conn.cursor()
cur.execute("SELECT dataownercode,destinationcode,destinationname50 from destination", [])
rows = cur.fetchall()
for row in rows:
    destination_id = row[0] + '_' + row[1]
    destination = {'_id' : destination_id, intern('DestinationName50') : row[2]}
    destinationmeta_store.update({'_id' : destination_id},destination,True)
cur.close()

cur = conn.cursor()
cur.execute("select timingpointcode,timingpointname,timingpointtown,stopareacode,CAST(ST_Y(the_geom) AS NUMERIC(8,6)) AS lat,CAST(ST_X(the_geom) AS NUMERIC(7,6)) AS lon FROM (select distinct t.timingpointcode as timingpointcode, t.timingpointname as timingpointname, t.timingpointtown as timingpointtown,t.stopareacode as stopareacode,ST_Transform(st_setsrid(st_makepoint(locationx_ew, locationy_ns), 28992), 4326) AS the_geom from timingpoint as t where not exists (select 1 from usertimingpoint,localservicegrouppasstime where t.timingpointcode = usertimingpoint.timingpointcode and journeystoptype = 'INFOPOINT' and usertimingpoint.dataownercode = localservicegrouppasstime.dataownercode and usertimingpoint.userstopcode = localservicegrouppasstime.userstopcode)) as W;",[])
kv7rows = cur.fetchall()
for kv7row in kv7rows:
    tpc = {'_id' : kv7row[0] ,'TimingPointName' : kv7row[1], 'TimingPointTown' : kv7row[2], 'StopAreaCode' : kv7row[3], 'Latitude' : str(kv7row[4]), 'Longitude' : str(kv7row[5])} 
    if kv7row[3] == None:
       del(tpc['StopAreaCode'])
    tpcmeta_store.update({'_id' : kv7row[0]},tpc,True)
cur.close()
conn.close()

def toisotime(operationdate, timestamp, row):
    hours, minutes, seconds = timestamp.split(':')
    if hours == 0 and minutes == 0 and seconds == 0:
    	    return '0000-00-00T00:00'
    hours = int(hours)
    if hours >= 48:
        print row

    if hours >= 24:
        deltadays  = hours / 24
        hours = hours % 24
        years, months, days = operationdate.split('-')
        return (datetime(int(years), int(months), int(days), hours, int(minutes), int(seconds)) + timedelta(days = deltadays)).isoformat()
    else:
        return operationdate+'T'+timestamp

def cleanup(): 	    
    now = datetime.today() - timedelta(seconds=90)
    time = now.strftime("%Y-%m-%dT%H:%M:%S")
    passtimes_store.remove({'ExpectedArrivalTime' : {'$lt' : time} })

    for line_id, line in line_store.items():
    	    for id, row in line['Actuals'].items():
    	    	    if now > datetime.strptime(row['ExpectedArrivalTime'], "%Y-%m-%dT%H:%M:%S") and now > datetime.strptime(row['ExpectedDepartureTime'], "%Y-%m-%dT%H:%M:%S"):
    	    	    	    del line['Actuals'][id]
        
def fetchkv7(row):
        try:
                conn = psycopg2.connect("dbname='kv78turbo' user='postgres' port='5433'")
	except:
                conn = psycopg2.connect("dbname='kv78turbo1' user='postgres' port='5433'")
	cur = conn.cursor()
	cur.execute("SELECT userstopordernumber, targetarrivaltime, targetdeparturetime, productformulatype from localservicegrouppasstime as ""p"" WHERE p.dataownercode = %s and localservicelevelcode = %s and journeynumber = %s and fortifyordernumber = %s and p.lineplanningnumber = %s and userstopcode = %s LIMIT 1;", [row['DataOwnerCode'],row['LocalServiceLevelCode'], row['JourneyNumber'], row['FortifyOrderNumber'], row['LinePlanningNumber'], row['UserStopCode']])
	kv7rows = cur.fetchall()
	for kv7row in kv7rows:
		row['TargetArrivalTime'] = toisotime(row['OperationDate'], kv7row[1], row)
		row['TargetDepartureTime'] = toisotime(row['OperationDate'], kv7row[2], row)
    		row['ProductFormulaType'] = kv7row[3]
        cur.close()	
        conn.close()

def storecurrect(row): 	    
    if row['TripStopStatus'] != 'UNKNOWN' and row['TripStopStatus'] != 'PLANNED': #Keeps status of the dataowners supplying us data
            last_updatedataownerstore[row['DataOwnerCode']] = row['LastUpdateTimeStamp']
    elif row['DataOwnerCode'] not in last_updatedataownerstore:
            last_updatedataownerstore[row['DataOwnerCode']] = 'ERROR'
   	    	    
    id = '_'.join([row['DataOwnerCode'], row['LocalServiceLevelCode'], row['LinePlanningNumber'], row['JourneyNumber'], row['FortifyOrderNumber']])
    line_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber'] + '_' + row['LineDirection']
    linemeta_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber']
    destinationmeta_id = row['DataOwnerCode'] + '_' + row['DestinationCode']
    pass_id = '_'.join([row['UserStopCode'], row['UserStopOrderNumber']])
    key = id + '_' + pass_id
  
    if row['TripStopStatus'] == 'CANCEL': #debug for testing CANCELED passes
    	    print 'CANCEL ' + id
            print 'XCANCEL'+ row['LastUpdateTimeStamp'] + '  ' + row['ExpectedArrivalTime'] + ' ' + id + '_' + pass_id 

    row['ExpectedArrivalTime'] = toisotime(row['OperationDate'], row['ExpectedArrivalTime'], row)
    row['ExpectedDepartureTime'] = toisotime(row['OperationDate'], row['ExpectedDepartureTime'], row)
    row['LocalServiceLevelCode'] = int(row['LocalServiceLevelCode'])    
    if row['TripStopStatus'] == 'CANCEL': #TODO IMPROVE!
    	    try:
    	    	    fetchkv7(row) #fetch KV7 as CANCEL message may be too much ahead of the KV7 feed and thus are overwritten and we wish KV7 info for CANCEL's
    	    except:
    	    	    pass  	    	              
    try:
        for x in ['JourneyNumber', 'FortifyOrderNumber', 'UserStopOrderNumber', 'NumberOfCoaches', 'LocalServiceLevelCode']:
            if x in row and row[x] is not None and row[x] != 'UNKNOWN':
                row[x] = int(row[x])
            else:
                del(row[x])       
        row['IsTimingStop'] = (row['IsTimingStop'] == '1')
    except:
        pass
        #raise

    tpc_meta = tpcmeta_store.find_one({'_id' : row['TimingPointCode']})
      
    if row['TimingPointCode'] not in tpc_store:
    	    tpc_store[row['TimingPointCode']] = {'GeneralMessages' : {}, 'Passes' : {id : True}}
    elif id not in tpc_store[row['TimingPointCode']]['Passes']:
    	    tpc_store[row['TimingPointCode']]['Passes'][id] = True
    
    if tpc_meta != None:    
            del tpc_meta['_id']	    
    	    row.update(tpc_meta)
     
    destination_meta = destinationmeta_store.find_one({'_id' : destinationmeta_id})
   
    if destination_meta != None:
            del destination_meta['_id']
    	    row['DestinationName50'] = destination_meta['DestinationName50']
    
    if 'StopAreaCode' in row and row['StopAreaCode'] != None:
    	    if row['StopAreaCode'] not in stopareacode_store:
    	    	    stopareacode_store[row['StopAreaCode']] = { row['TimingPointCode'] : {'Passes' : {}}}
    	    elif row['TimingPointCode'] not in stopareacode_store[row['StopAreaCode']]:
    	    	    stopareacode_store[row['StopAreaCode']][row['TimingPointCode']] = {'Passes' : {}}
    
    if line_id not in line_store:
    	line_store[line_id] = {'Network': {}, 'Actuals': {}, 'Line' : {}}
    	line_store[line_id]['Line'] = {'DataOwnerCode' : row['DataOwnerCode']}
    	line_store[line_id]['Line']['LineDirection'] = row['LineDirection']
    	line_store[line_id]['Line']['LinePlanningNumber'] = row['LinePlanningNumber']

    line_meta = linemeta_store.find_one({'_id' : linemeta_id})  
    
    if line_meta != None:
	    del line_meta['_id']
    	    row.update(line_meta)
    	    line_store[line_id]['Line'].update(line_meta)
    
    if 'DestinationName50' in row:
    	    line_store[line_id]['Line']['DestinationName50'] = row['DestinationName50']
    elif 'DestinationName50' in line_store[line_id]:
    	    del(line_store[line_id]['Line']['DestinationName50'])
    		
    if row['UserStopOrderNumber'] not in line_store[line_id]['Network']:
        line_store[line_id]['Network'][row['UserStopOrderNumber']] = {
            'TimingPointCode': row['TimingPointCode'],
            'IsTimingStop': row['IsTimingStop'],
            'UserStopOrderNumber':row['UserStopOrderNumber']
            }
        if tpc_meta != None:
            line_store[line_id]['Network'][row['UserStopOrderNumber']].update(tpc_meta) #add tpc metainfo and userstopordernumber to line network

    if id not in journey_store:
    	journey_store[id] = {'Stops' : {}}
    else:
        journey_store[id]['Stops'][row['UserStopOrderNumber']] = {}

    if row['TripStopStatus'] in set(['ARRIVED', 'PASSED']): # , 'DRIVING']): Driving alleen nemen als kleinste waarde uit lijn, gegeven dat er geen ARRIVED/PASSED is
    	for key in journey_store[id]['Stops'].keys(): #delete previous stops from journey
            if key < int(row['UserStopOrderNumber']) - 1:
            	del(journey_store[id]['Stops'][key])

        if row['JourneyStopType'] == 'LAST': #delete journey
            if id in line_store[line_id]['Actuals']:
                del(line_store[line_id]['Actuals'][id])
        else:
            line_store[line_id]['Actuals'][id] = row
    elif row['TripStopStatus'] == 'DRIVING':   #replace a passed stop with the next stop
    	previousStopOrder = int(row['UserStopOrderNumber']) - 1
    	#if previousStopOrder in journey_store[id]['Stops'] and journey_store[id]['Stops'][previousStopOrder]['TripStopStatus'] == 'PASSED':
    	#    line_store[line_id]['Actuals'][id] = row
    elif row['TripStopStatus'] == 'PLANNED' and id not in line_store[line_id]['Actuals'] and int(row['UserStopOrderNumber']) == 1: #add planned journeys
    	line_store[line_id]['Actuals'][id] = row
    elif (row['TripStopStatus'] == 'UNKNOWN' or row['TripStopStatus'] == 'CANCEL') and id in line_store[line_id]['Actuals']: #Delete canceled or non live journeys
	del(line_store[line_id]['Actuals'][id])
    if row['SideCode'] == '-':
	del(row['SideCode'])

    row["_id"] = key
    passtimes_store.update({"_id" : key},row,True)
            
def storeplanned(row):
	row['TargetArrivalTime'] = toisotime(row['OperationDate'], row['TargetArrivalTime'], row)
	row['TargetDepartureTime'] = toisotime(row['OperationDate'], row['TargetDepartureTime'], row)
	row['ProductFormulaType'] = int(row['ProductFormulaType'])
        row['DataOwnerCode'] = intern(row['DataOwnerCode'])
        row['LocalServiceLevelCode'] = intern(row['LocalServiceLevelCode'])
        row['OperationDate'] = intern(row['OperationDate'])
        row['WheelChairAccessible'] = intern(row['WheelChairAccessible'])
        row['JourneyStopType'] = intern(row['JourneyStopType'])
        row['UserStopCode'] = intern(row['UserStopCode'])
        row['DestinationCode'] = intern(row['DestinationCode'])
        row['TimingPointCode'] = intern(row['TimingPointCode'])
        row['SideCode'] = intern(row['SideCode'])
        row['LinePlanningNumber'] = intern(row['LinePlanningNumber'])
        row['JourneyNumber'] = intern(row['JourneyNumber'])
        row['LineDirection'] = intern(row['LineDirection'])
        row['TripStopStatus'] = intern(row['TripStopStatus'])
       	storecurrect(row)
        	
def storemessage(row):
        id = '_'.join([row['DataOwnerCode'], row['MessageCodeDate'], row['MessageCodeNumber'], row['TimingPointDataOwnerCode'], row['TimingPointCode']])
        if row['TimingPointCode'] in tpc_store:
        	tpc_store[row['TimingPointCode']]['GeneralMessages'][id] = row
        else:
        	tpc_store[row['TimingPointCode']] = {'Passes' : {}, 'GeneralMessages' : {id : row}}
	generalmessagestore[id] = row

def deletemessage(row):
        id = '_'.join([row['DataOwnerCode'], row['MessageCodeDate'], row['MessageCodeNumber'], row['TimingPointDataOwnerCode'], row['TimingPointCode']])
        if row['TimingPointCode'] in tpc_store and id in tpc_store[row['TimingPointCode']]['GeneralMessages']:
        	del(tpc_store[row['TimingPointCode']]['GeneralMessages'][id])
        if id in generalmessagestore:
        	del(generalmessagestore[id])
        	
def queryTimingPoints(arguments):
	if len(arguments) == 1:
                reply = {}
                for tpc, values in tpc_store.items():
                	reply[tpc] = len(values['Passes'])
                return reply
        else:
                reply = {}
		tpccodes = list(set(arguments[1].split(',')))
		tpccodes_meta = tpcmeta_store.find({'_id' : {'$in' : tpccodes}})
                for tpc_meta in tpccodes_meta:
                        reply[tpc_meta['_id']] = { 'Stop' : tpc_meta, 'Passes' : {}}
                        if tpc_meta['_id'] in tpc_store:
				reply[tpc_meta['_id']]['GeneralMessages'] = tpc_store[tpc_meta['_id']]['GeneralMessages']
			else:
				reply[tpc_meta['_id']]['GeneralMessages'] = {}
                        del(tpc_meta['_id'])
                for row in passtimes_store.find({'TimingPointCode' : { '$in': tpccodes}}):
			id = '_'.join([row['DataOwnerCode'], str(row['LocalServiceLevelCode']), row['LinePlanningNumber'], str(row['JourneyNumber']), str(row['FortifyOrderNumber'])])
                        del(row['_id'])
                        if row['TimingPointCode'] not in reply:
                                reply[row['TimingPointCode']] = {'Passes' : {id : row}} 
                        elif 'Passes' in reply[row['TimingPointCode']]:
                        	reply[row['TimingPointCode']]['Passes'][id] = row
                        else:
                        	reply[row['TimingPointCode']]['Passes'] = {id : row }
		return reply
	
def queryJourneys(arguments):
	if len(arguments) == 1:
                reply = {}
                for journey, values in journey_store.items():
                	reply[journey] = len(values['Stops'])
                return reply
        else:
                reply = {}
                journeys = set(arguments[1].split(','))
                for journey in journeys:
                        if journey in journey_store:
                	    key = journey.split('_')
                            for stop in passtimes_store.find({'DataOwnerCode' : key[0], 'LocalServiceLevelCode' : int(key[1]), 'LinePlanningNumber' : key[2], 'JourneyNumber' : int(key[3]), 'FortifyOrderNumber' : int(key[4])}):
                		del(stop['_id'])
                                if journey in reply :
                			reply[journey]['Stops'][stop['UserStopOrderNumber']] = stop
                		else:
                			reply[journey] = {'Stops' : {stop['UserStopOrderNumber'] : stop}}
                return reply

def queryStopAreaCodes (arguments):
        if len(arguments) == 1:
                reply = {}
                for stopareacode, values in stopareacode_store.items():
                        for tpc, tpcvalues in stopareacode_store[stopareacode].items():
                                tpc_meta = tpcmeta_store.find_one({'_id' : tpc})
                                if tpc_meta != None:
                                      del(tpc_meta['_id'])
                                      reply[stopareacode] = tpc_meta
                return reply
        else:
                reply = {}
                stopareas = list(set(arguments[1].split(',')))
                for row in passtimes_store.find({'StopAreaCode' : { '$in': stopareas}}):
			id = '_'.join([row['DataOwnerCode'], str(row['LocalServiceLevelCode']), row['LinePlanningNumber'], str(row['JourneyNumber']), str(row['FortifyOrderNumber'])])
                        del(row['_id'])
                        stopareacode = row['StopAreaCode']
                        if stopareacode not in reply:
                                reply[stopareacode] = {row['TimingPointCode'] : {'Passes' : {id : row}}}
                        if row['TimingPointCode'] not in reply[stopareacode]:
                        	reply[stopareacode] = {row['TimingPointCode'] : {'Passes' : {id : row}}}
                        elif 'Passes' in reply[stopareacode][row['TimingPointCode']]:
                        	reply[stopareacode][row['TimingPointCode']]['Passes'][id] = row
                        else:
                        	reply[stopareacode][row['TimingPointCode']]['Passes'] = {id : row }
                return reply  
                
def queryLines(arguments):
	if len(arguments) == 1:
                reply = {}
                for line, values in line_store.items():
                      reply[line] = values['Line']
                return reply
        else:
                reply = {}
                for line in set(arguments[1].split(',')):
                    if line in line_store and line != '':
                        reply[line] = line_store[line]
                return reply

context = zmq.Context()

client = context.socket(zmq.REP)
client.bind(ZMQ_KV78UWSGI)

kv8 = context.socket(zmq.SUB)
kv8.connect(ZMQ_KV8)
kv8.setsockopt(zmq.SUBSCRIBE, "/GOVI/KV8")

kv7 = context.socket(zmq.PULL)
kv7.bind(ZMQ_KV7)

poller = zmq.Poller()
poller.register(client, zmq.POLLIN)
poller.register(kv8, zmq.POLLIN)
poller.register(kv7, zmq.POLLIN)

garbage = 0

while True:
    socks = dict(poller.poll())
    
    if socks.get(kv8) == zmq.POLLIN:
        multipart = kv8.recv_multipart()
        content = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
        c = ctx(content)
        if 'DATEDPASSTIME' in c.ctx:
            for row in c.ctx['DATEDPASSTIME'].rows():
                    if row['TripStopStatus'] == 'CANCEL':
                        print content
                    if row['MessageContent'] == None:
                       	del(row['MessageContent'])
                    if row['SubReasonType'] == None:
                      	del(row['SubReasonType'])
                    if row['ReasonType'] == None:
                       	del(row['ReasonType'])
                    if row['AdviceType'] == None:
                      	del(row['AdviceType'])
                    if row['AdviceContent'] == None:
                      	del(row['AdviceContent'])
                    if row['SubAdviceType'] == None:
                       	del(row['SubAdviceType'])
        	    if row['MessageType'] == None:
        		del(row['MessageType'])
        	    if row['ReasonContent'] == None:
        		del(row['ReasonContent'])
                    storecurrect(row)
        if 'GENERALMESSAGEUPDATE' in c.ctx:
            sys.stdout.write('MSGUPDATE')
            sys.stdout.flush()
            for row in c.ctx['GENERALMESSAGEUPDATE'].rows():
            	    storemessage(row)
        if 'GENERALMESSAGEDELETE' in c.ctx:
            sys.stdout.write('MSGDELETE')
            sys.stdout.flush()
            for row in c.ctx['GENERALMESSAGEDELETE'].rows():
            	    deletemessage(row)

    elif socks.get(kv7) == zmq.POLLIN:
    	data = kv7.recv_json()
        for pass_id, row in data.items():
        	storeplanned(row)

    elif socks.get(client) == zmq.POLLIN:
        url = client.recv()
        arguments = url.split('/')
        print url
        if arguments[0] == 'tpc':
        	reply = queryTimingPoints(arguments)
                client.send_json(reply) 
                
        elif arguments[0] == 'journey':
        	reply = queryJourneys(arguments)
                client.send_json(reply) 
                
        elif arguments[0] == 'stopareacode':
        	reply = queryStopAreaCodes(arguments)
        	client.send_json(reply) 
                
        elif arguments[0] == 'line':
        	reply = queryLines(arguments)
        	client.send_json(reply) 
                
        elif arguments[0] == 'lastupdate':
            reply = {'LastUpdateTimeStamps' : last_updatedataownerstore, 'ServerTime' : strftime("%Y-%m-%dT%H:%M:%SZ",gmtime())}
            client.send_json(reply)
            
        elif arguments[0] == 'generalmessage':
            client.send_json(generalmessagestore)
            
        else:
            client.send_json([])

    if garbage > 120:
        cleanup()
        #sys.stdout.write('c')
        #sys.stdout.flush()
        garbage = 0
    else:
        garbage += 1
