import sys
import time
import zmq
from const import ZMQ_KV8, ZMQ_KV78UWSGI, ZMQ_KV7
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
    line = {'LinePublicNumber' : row[2], 'LineName' : row[3], 'TransportType' : row[4], '_id' : line_id}
    linemeta_store.save(line)
cur.close()

cur = conn.cursor()
cur.execute("SELECT dataownercode,destinationcode,destinationname50 from destination", [])
rows = cur.fetchall()
for row in rows:
    destination_id = row[0] + '_' + row[1]
    destination = {'_id' : destination_id, 'DestinationName50' : row[2]}
    destinationmeta_store.save(destination)
cur.close()

cur = conn.cursor()
cur.execute("select timingpointcode,timingpointname,timingpointtown,stopareacode,CAST(ST_Y(the_geom) AS NUMERIC(9,7)) AS lat,CAST(ST_X(the_geom) AS NUMERIC(8,7)) AS lon,motorisch,visueel FROM (select distinct t.timingpointcode as timingpointcode,motorisch,visueel, t.timingpointname as timingpointname, t.timingpointtown as timingpointtown,t.stopareacode as stopareacode,ST_Transform(st_setsrid(st_makepoint(t.locationx_ew, t.locationy_ns), 28992), 4326) AS the_geom from timingpoint as t left join haltescan as h on (t.timingpointcode = h.timingpointcode) where not exists (select 1 from usertimingpoint,localservicegrouppasstime where t.timingpointcode = usertimingpoint.timingpointcode and journeystoptype = 'INFOPOINT' and usertimingpoint.dataownercode = localservicegrouppasstime.dataownercode and usertimingpoint.userstopcode = localservicegrouppasstime.userstopcode)) as W;",[])
kv7rows = cur.fetchall()
for kv7row in kv7rows:
    tpc = { '_id' : kv7row[0], 'TimingPointName' : kv7row[1], 'TimingPointTown' : kv7row[2]), 'StopAreaCode' : kv7row[3], 'Latitude' : float(kv7row[4]), 'Longitude' : float(kv7row[5])}
    if not (kv7row[6] == None and kv7row[7] == None):
       tpc['TimingPointWheelChairAccessible'] = kv7row[6]
       tpc['TimingPointVisualAccessible'] = kv7row[7]
    tpcmeta_store.update({'_id' : kv7row[0]},tpc,True)
cur.close()
conn.close()

def totimestamp(operationdate, timestamp, row):
    hours, minutes, seconds = timestamp.split(':')   
    years, months, days = operationdate.split('-')
    if hours == 0 and minutes == 0 and seconds == 0:
        return int(0)
    hours = int(hours)
    if hours >= 48:
        print row
    if hours >= 24:
        deltadays  = hours / 24
        hours = hours % 24
        return int((datetime(int(years), int(months), int(days), hours, int(minutes), int(seconds)) + timedelta(days = deltadays)).strftime("%s"))
    else:
        return int(datetime(int(years), int(months), int(days), hours, int(minutes), int(seconds)).strftime("%s"))
 
def todate(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")

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

    if 'LastUpdateTimeStamp' in row:
        date,time = row['LastUpdateTimeStamp'].split('T')
        row['LastUpdateTimeStamp'] = totimestamp(date,time[:-6],None)

    id = '_'.join([row['DataOwnerCode'], str(row['LocalServiceLevelCode']), row['LinePlanningNumber'], str(row['JourneyNumber']), str(row['FortifyOrderNumber'])])
    line_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber'] + '_' + str(row['LineDirection'])
    linemeta_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber']
    destinationmeta_id = row['DataOwnerCode'] + '_' + row['DestinationCode']
    pass_id = '_'.join([row['UserStopCode'], str(row['UserStopOrderNumber'])])
    key = id + '_' + pass_id

    row['ExpectedArrivalTime'] = totimestamp(row['OperationDate'], row['ExpectedArrivalTime'], row)
    row['ExpectedDepartureTime'] = totimestamp(row['OperationDate'], row['ExpectedDepartureTime'], row)

    oldrow = passtimes_store.find_one({'_id' : key})
    if oldrow != None:
            oldrow.update(row)
            row = oldrow 

    if row['TripStopStatus'] == 'CANCEL':
        try:
    	    fetchkv7(row)
            pass
        except:
            print 'KV7 fetch error'
            pass

    for x in ['JourneyNumber', 'FortifyOrderNumber', 'UserStopOrderNumber', 'NumberOfCoaches', 'LocalServiceLevelCode', 'LineDirection']:
        try:
            if x in row and row[x] is not None and row[x] != 'UNKNOWN':
	        row[x] = int(row[x])
	    else:
                del(row[x])
        except:
	    pass
    row['IsTimingStop'] = (row['IsTimingStop'] == '1')

    if row['WheelChairAccessible'] == 'ACCESSIBLE' and row['TimingPointCode'] in tpc_meta:
        tpc_meta[row['TimingPointCode']]['TimingPointWheelChairAccessible'] = True
    
    if row['TimingPointCode'] not in tpc_store:
    	    tpc_store[row['TimingPointCode']] = {'GeneralMessages' : {}, 'Passes' : {id : True}}
    elif id not in tpc_store[row['TimingPointCode']]['Passes']:
    	    tpc_store[row['TimingPointCode']]['Passes'][id] = True

    if row['TimingPointCode'] in tpc_meta:
        stopareacode = tpc_meta = tpcmeta_store.find_one({'_id' : row['TimingPointCode']})['StopAreaCode']
        if stopareacode != None:
    	    if stopareacode not in stopareacode_store:
    	    	    stopareacode_store[stopareacode] = { row['TimingPointCode'] : {'Passes' : {}}}
    	    elif row['TimingPointCode'] not in stopareacode_store[stopareacode]:
    	    	    stopareacode_store[stopareacode][row['TimingPointCode']] = {'Passes' : {}}
    
    if line_id not in line_store:
    	line_store[line_id] = {'Network': {}, 'Actuals': {}, 'Line' : {}}
    	line_store[line_id]['Line'] = {'DataOwnerCode' : row['DataOwnerCode']}
    	line_store[line_id]['Line']['LineDirection'] = row['LineDirection']
    	line_store[line_id]['Line']['LinePlanningNumber'] = row['LinePlanningNumber']
        line_meta = linemeta_store.find_one({'_id' : linemeta_id})  
        destination_meta = destinationmeta_store.find_one({'_id' : destinationmeta_id})
        line_meta = linemeta_store.find_one({'_id' : linemeta_id})  
        if line_meta != None:
            line_store[line_id]['Line'].update(line_meta[linemeta_id])
        if destination_meta != None:
            line_store[line_id]['Line']['DestinationName50'] = destination_meta['DestinationName50']
          		
    if row['UserStopOrderNumber'] not in line_store[line_id]['Network']:
        line_store[line_id]['Network'][row['UserStopOrderNumber']] = {
            'TimingPointCode': row['TimingPointCode'],
            'IsTimingStop': row['IsTimingStop'],
            'UserStopOrderNumber':row['UserStopOrderNumber']
            }
        if row['TimingPointCode'] in tpc_meta:
            line_store[line_id]['Network'][row['UserStopOrderNumber']].update(tpc_meta[row['TimingPointCode']]) 
    if id not in journey_store:
    	journey_store[id] = {'Stops' : {row['UserStopOrderNumber']: row}}
    else:
        journey_store[id]['Stops'][row['UserStopOrderNumber']] = row

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
    	if previousStopOrder in journey_store[id]['Stops'] and journey_store[id]['Stops'][previousStopOrder]['TripStopStatus'] == 'PASSED':
    	    line_store[line_id]['Actuals'][id] = row
    elif row['TripStopStatus'] == 'PLANNED' and id not in line_store[line_id]['Actuals'] and int(row['UserStopOrderNumber']) == 1: #add planned journeys
    	line_store[line_id]['Actuals'][id] = row
    elif (row['TripStopStatus'] == 'UNKNOWN' or row['TripStopStatus'] == 'CANCEL') and id in line_store[line_id]['Actuals']: #Delete canceled or non live journeys
	del(line_store[line_id]['Actuals'][id])
    if 'SideCode' in row and row['SideCode'] == '-':
	del(row['SideCode'])
            
def storeplanned(row):
    id = '_'.join([row['DataOwnerCode'], str(row['LocalServiceLevelCode']), row['LinePlanningNumber'], str(row['JourneyNumber']), str(row['FortifyOrderNumber'])])
    pass_id = '_'.join([row['UserStopCode'], str(row['UserStopOrderNumber'])])
    if id not in kv7cache or pass_id not in kv7cache:
        if id not in kv7cache:
            kv7cache[id] = {pass_id : {'TargetArrivalTime' : totimestamp(row['OperationDate'], row['TargetArrivalTime'], row)}}
        else:
            kv7cache[id][pass_id] = { 'TargetArrivalTime' : totimestamp(row['OperationDate'], row['TargetArrivalTime'], row)}
    kv7cache[id][pass_id]['TargetDepartureTime'] = totimestamp(row['OperationDate'], row['TargetDepartureTime'], row)
    kv7cache[id][pass_id]['ProductFormulaType'] = int(row['ProductFormulaType'])
    row['DataOwnerCode'] = intern(row['DataOwnerCode'])
    row['OperationDate'] = intern(row['OperationDate'])
    row['WheelChairAccessible'] = intern(row['WheelChairAccessible'])
    row['JourneyStopType'] = intern(row['JourneyStopType'])
    row['UserStopCode'] = intern(row['UserStopCode'])
    row['DestinationCode'] = intern(row['DestinationCode'])
    row['TimingPointCode'] = intern(row['TimingPointCode'])
    row['LinePlanningNumber'] = intern(row['LinePlanningNumber'])
    row['TripStopStatus'] = intern(row['TripStopStatus'])
    if 'SideCode' in row and row['SideCode'] == '-':
        del(row['SideCode'])
    elif 'SideCode' in row:
        row['SideCode'] = intern(row['SideCode'])
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

def addMeta(passtimes):
    result = {}
    for key, values in passtimes.items():
        result[key] = values.copy()
        linemeta_id = values['DataOwnerCode'] + '_' + values['LinePlanningNumber']
        if linemeta_id in line_meta:
            result[key].update(line_meta[linemeta_id])
        destinationmeta_id = values['DataOwnerCode'] + '_' + values['DestinationCode']
        if destination_id in destination_meta:
            result[key]['DestinationName50'] = destination_meta[destinationmeta_id]
        timingpointcode = values['TimingPointCode']
        if timingpointcode in tpc_meta:
            result[key].update(tpc_meta[timingpointcode])
        result[key]['ExpectedDepartureTime'] = todate(result[key]['ExpectedDepartureTime'])
        result[key]['ExpectedArrivalTime'] = todate(result[key]['ExpectedArrivalTime'])
        if 'TargetDepartureTime' in values:
            result[key]['TargetDepartureTime'] = todate(result[key]['TargetDepartureTime'])
        if 'TargetArrivalTime' in values:
            result[key]['TargetArrivalTime'] = todate(result[key]['TargetArrivalTime'])
        if 'LastUpdateTimeStamp' in values:
            result[key]['LastUpdateTimeStamp'] = todate(result[key]['LastUpdateTimeStamp'])
    return result

def queryTimingPoints(arguments):
    if arguments[0] == 'tpc':
        if len(arguments) == 1:
            reply = {}
            for tpc, values in tpc_store.items():
                reply[tpc] = len(values['Passes'])
            return reply
        else:
            reply = {}
            for tpc in set(arguments[1].split(',')):
                if tpc in tpc_store and tpc != '':
                        reply[tpc] = tpc_store[tpc].copy()
                        reply[tpc]['Passes'] = addMeta(reply[tpc]['Passes'])
                if tpc in tpc_meta and tpc != '':
                    if tpc in reply:
                        reply[tpc]['Stop'] = tpc_meta[tpc]
                    else:
    	    	        tpc_store[tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
            return reply

def queryJourneys(arguments):
    if len(arguments) == 1:
        reply = {}
        for journey, values in journey_store.items():
            reply[journey] = len(values['Stops'])
        return reply
    else:
        reply = {}
        for journey in set(arguments[1].split(',')):
            if journey in journey_store:
                if journey != '':
                    reply[journey] = journey_store[journey].copy()
                    reply[journey]['Stops'] = addMeta(reply[journey]['Stops'])
        return reply

def queryStopAreas(arguments):
    if len(arguments) == 1:
        reply = {}
        for stopareacode, values in stopareacode_store.items():
            for tpc, tpcvalues in stopareacode_store[stopareacode].items():
                if tpc in tpc_meta:
                    reply[stopareacode] = tpc_meta[tpc]
        return reply
    else:
        reply = {}
        for stopareacode in set(arguments[1].split(',')):
            if stopareacode in stopareacode_store and stopareacode != '':
                reply[stopareacode] = stopareacode_store[stopareacode].copy()
                for tpc, tpcvalues in stopareacode_store[stopareacode].items():
                    reply[stopareacode][tpc]['Passes'] = addMeta(reply[stopareacode][tpc]['Passes'])
        return reply
   	
def queryLines(arguments):
    if len(arguments) == 1:
        reply = {}
        for line, values in line_store.items():
            reply[line] = values['Line'].copy()
            linemeta_id = values['Line']['DataOwnerCode'] + '_' + values['Line']['LinePlanningNumber']
            if linemeta_id in line_meta:
                reply[line].update(line_meta[linemeta_id])
        return reply
    else:
        reply = {}
        for line in set(arguments[1].split(',')):
            if line in line_store and line != '':
                reply[line] = line_store[line].copy()
                reply[line]['Actuals'] = addMeta(reply[line]['Actuals'])
        return reply

def recvPackage(content):
    for line in content.split('\r\n')[:-1]:
        if line[0] == '\\':
                # control characters
            if line[1] == 'G':
                label, Name, Comment, Path, Endian, Enc, Res1, TimeStamp, _ = line[2:].split('|')
            elif line[1] == 'T':
                type = line[2:].split('|')[1]
            elif line[1] == 'L':
                keys = line[2:].split('|')
        else:
            row = {}
            values = line.split('|')
            for k,v in map(None, keys, values):
                if v == '\\0':
                    v = None
                else:              
                    row[k] = v
            if row['TripStopStatus'] == 'CANCEL':
                print 'XCANCEL'+ row['LastUpdateTimeStamp'] + '  ' + row['ExpectedArrivalTime']
                print content
            for x in ['ReasonType', 'AdviceType', 'AdviceContent','SubAdviceType','MessageType','ReasonContent','OperatorCode', 'SubReasonType', 'MessageContent']:
                if x in row and row[x] is None:
                    del(row[x])
            if type == 'DATEDPASSTIME':
                storecurrect(row)
            elif type == 'GENERALMESSAGEUPDATE':
                print 'GENERAL MESSAGE UPDATE'
                storemessage(row)
            elif type == 'GENERALMESSAGEDELETE':
                print 'GENERAL MESSAGE DELETE'
                deletemessage(row)

while True:
    socks = dict(poller.poll())
    
    if socks.get(kv8) == zmq.POLLIN:
        multipart = kv8.recv_multipart()
        content = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
        recvPackage(content)
    elif socks.get(kv7) == zmq.POLLIN:
    	data = kv7.recv_json()
        for pass_id, row in data.items():
        	storeplanned(row)

    elif socks.get(client) == zmq.POLLIN:
        url = client.recv()
        arguments = url.split('/')
        if arguments[0] == 'tpc':
            reply = queryTimingPoints(arguments)
            client.send_json(reply)               
        elif arguments[0] == 'journey':
            reply = queryJourneys(arguments)
            client.send_json(reply)
        elif arguments[0] == 'stopareacode':
            reply = queryStopAreas(arguments)
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
        garbage = 0
    else:
        garbage += 1
