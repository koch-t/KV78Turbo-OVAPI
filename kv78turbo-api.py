import sys
import time
import zmq
from const import ZMQ_KV8, ZMQ_KV78UWSGI, ZMQ_KV7
from datetime import datetime, timedelta
from time import strftime, gmtime
from gzip import GzipFile
from cStringIO import StringIO
import psycopg2
from copy import deepcopy

conn = psycopg2.connect("dbname='kv78turbo'")

tpc_store = {}
stopareacode_store = {}
line_store = {}
journey_store = {}
last_updatestore = {'DataOwner' : {}, 'Subscription' : {}}
generalmessagestore = {}

tpc_meta = {}
line_meta = {}
destination_meta = {}

cur = conn.cursor()
cur.execute("SELECT dataownercode,lineplanningnumber,linepublicnumber,linename,transporttype from line", [])
rows = cur.fetchall()
for row in rows:
    line_id = intern(row[0] + '_' + row[1])
    line_meta[line_id] = {'LinePublicNumber' : intern(row[2]), 'LineName' : intern(row[3]), 'TransportType' : intern(row[4])}
cur.close()

cur = conn.cursor()
cur.execute("SELECT dataownercode,destinationcode,destinationname50 from destination", [])
rows = cur.fetchall()
for row in rows:
    destination_id = intern(row[0] + '_' + row[1])
    destination_meta[destination_id] = intern(row[2])
cur.close()

cur = conn.cursor()
cur.execute("select timingpointcode,timingpointname,timingpointtown,stopareacode,CAST(ST_Y(the_geom) AS NUMERIC(9,7)) AS lat,CAST(ST_X(the_geom) AS NUMERIC(8,7)) AS lon,motorisch,visueel FROM (select distinct t.timingpointcode as timingpointcode,motorisch,visueel, t.timingpointname as timingpointname, t.timingpointtown as timingpointtown,t.stopareacode as stopareacode,ST_Transform(st_setsrid(st_makepoint(t.locationx_ew, t.locationy_ns), 28992), 4326) AS the_geom from timingpoint as t left join haltescan as h on (t.timingpointcode = h.timingpointcode) where not exists (select 1 from usertimingpoint,localservicegrouppasstime where t.timingpointcode = usertimingpoint.timingpointcode and journeystoptype = 'INFOPOINT' and usertimingpoint.dataownercode = localservicegrouppasstime.dataownercode and usertimingpoint.userstopcode = localservicegrouppasstime.userstopcode)) as W;",[])
kv7rows = cur.fetchall()
for kv7row in kv7rows:
    tpc_meta[intern(kv7row[0])] = {'TimingPointName' : intern(kv7row[1]), 'TimingPointTown' : intern(kv7row[2]), 'StopAreaCode' : kv7row[3], 'Latitude' : float(kv7row[4]), 'Longitude' : float(kv7row[5])}
    if not (kv7row[6] == None and kv7row[7] == None):
       tpc_meta[kv7row[0]]['TimingPointWheelChairAccessible'] = kv7row[6]
       tpc_meta[kv7row[0]]['TimingPointVisualAccessible'] = kv7row[7]
    if kv7row[2] == None:
       del(tpc_meta[row['TimingPointCode']]['StopAreaCode'])
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
    now = long((datetime.today() - timedelta(seconds=90)).strftime("%s"))
    for timingpointcode, values in tpc_store.items():
        for journey, row in values['Passes'].items():
            if now > row['ExpectedArrivalTime'] and now > row['ExpectedDepartureTime']:
                del(tpc_store[timingpointcode]['Passes'][journey])
                if row['TripStopStatus'] == 'PLANNED' or row['TripStopStatus'] == 'UNKNOWN':
                    userstoporder = row['UserStopOrderNumber']
                    if journey in journey_store and userstoporder in journey_store[journey]['Stops']:
                        if len(journey_store[journey]['Stops']) > 1:
                            del(journey_store[journey]['Stops'][userstoporder])
                        else:
                            del(journey_store[journey])
    for journey_id, values in journey_store.items():
        row = values['Stops'][max(values['Stops'].keys())]
        if now > row['ExpectedArrivalTime'] and now > row['ExpectedDepartureTime']:
            line_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber'] + '_' + str(row['LineDirection'])
    	    if line_id in line_store and journey_id in line_store[line_id]['Actuals']:
    	        del(line_store[line_id]['Actuals'][journey_id])
    	    if journey_id in journey_store:
    	        del(journey_store[journey_id])

def fetchkv7(row):
        try:
                conn = psycopg2.connect("dbname='kv78turbo'")
	except:
                conn = psycopg2.connect("dbname='kv78turbo'")
        id = '_'.join([row['DataOwnerCode'], row['LocalServiceLevelCode'], row['LinePlanningNumber'], row['JourneyNumber'], row['FortifyOrderNumber']])
	cur.execute("SELECT targetarrivaltime, targetdeparturetime, productformulatype from localservicegrouppasstime as ""p"" WHERE p.dataownercode = %s and localservicelevelcode = %s and journeynumber = %s and fortifyordernumber = %s and p.lineplanningnumber = %s and userstopcode = %s and userstopordernumber = %s LIMIT 1;", [row['DataOwnerCode'],row['LocalServiceLevelCode'], row['JourneyNumber'], row['FortifyOrderNumber'], row['LinePlanningNumber'], row['UserStopCode'], row['UserStopOrderNumber']])
	kv7rows = cur.fetchall()
	pass_id = '_'.join([row['UserStopCode'], row['UserStopOrderNumber']])
	if len(kv7rows) == 0:
                print "Not in KV7 " + id+'_'+pass_id
	for kv7row in kv7rows:
		row['TargetArrivalTime']   = totimestamp(row['OperationDate'], kv7row[0], row)
		row['TargetDepartureTime'] = totimestamp(row['OperationDate'], kv7row[1], row)
		row['ProductFormulaType']  = kv7row[2]
        cur.close()
        conn.close()

def storecurrect(newrow): 	    
    newrow['ExpectedArrivalTime'] = totimestamp(newrow['OperationDate'], newrow['ExpectedArrivalTime'], newrow)
    newrow['ExpectedDepartureTime'] = totimestamp(newrow['OperationDate'], newrow['ExpectedDepartureTime'], newrow)

    if 'LastUpdateTimeStamp' in newrow:
        date,time = newrow['LastUpdateTimeStamp'].split('T')
        newrow['LastUpdateTimeStamp'] = totimestamp(date,time[:-6],None)

    id = '_'.join([newrow['DataOwnerCode'], str(newrow['LocalServiceLevelCode']), newrow['LinePlanningNumber'], str(newrow['JourneyNumber']), str(newrow['FortifyOrderNumber'])])
    if id in journey_store and int(newrow['UserStopOrderNumber']) in journey_store[id]['Stops']:
        row = journey_store[id]['Stops'][int(newrow['UserStopOrderNumber'])]
        row.update(newrow)
    else:
        row = newrow

    line_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber'] + '_' + str(row['LineDirection'])
    linemeta_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber']
    destinationmeta_id = row['DataOwnerCode'] + '_' + row['DestinationCode']
    pass_id = '_'.join([row['UserStopCode'], str(row['UserStopOrderNumber'])])

    if row['TripStopStatus'] == 'CANCEL' and 'TargetArrivalTime' not in row:
        try:
    	    fetchkv7(row)
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
    	    tpc_store[row['TimingPointCode']] = {'Passes' : {id: row}, 'GeneralMessages' : {}}
    else:
    	    tpc_store[row['TimingPointCode']]['Passes'][id] = row

    if row['TimingPointCode'] in tpc_meta:
        stopareacode = tpc_meta[row['TimingPointCode']]['StopAreaCode']
        if stopareacode != None:
    	    if stopareacode not in stopareacode_store:
    	    	    stopareacode_store[stopareacode] = { row['TimingPointCode'] : True}
    	    elif row['TimingPointCode'] not in stopareacode_store[stopareacode]:
    	    	    stopareacode_store[stopareacode][row['TimingPointCode']] = True
    
    if line_id not in line_store:
    	line_store[line_id] = {'Network': {}, 'Actuals': {}, 'Line' : {}}
    	line_store[line_id]['Line'] = {'DataOwnerCode' : row['DataOwnerCode']}
    	line_store[line_id]['Line']['LineDirection'] = row['LineDirection']
    	line_store[line_id]['Line']['LinePlanningNumber'] = row['LinePlanningNumber']
        if linemeta_id in line_meta:
            line_store[line_id]['Line'].update(line_meta[linemeta_id])
    
    if destinationmeta_id in destination_meta:
        line_store[line_id]['Line']['DestinationName50'] = destination_meta[destinationmeta_id]      
        		
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
    row['TargetArrivalTime'] = totimestamp(row['OperationDate'], row['TargetArrivalTime'], row)
    row['TargetDepartureTime'] = totimestamp(row['OperationDate'], row['TargetDepartureTime'], row)
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
    	    	        reply[tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
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
                reply[stopareacode] = deepcopy(stopareacode_store[stopareacode])
                for tpc, tpcvalues in reply[stopareacode].items():
                    if tpc in tpc_store and tpc != '':
                        reply[stopareacode][tpc] = tpc_store[tpc].copy()
                        reply[stopareacode][tpc]['Passes'] = addMeta(reply[stopareacode][tpc]['Passes'])
                        if tpc in tpc_meta:
                            if tpc in reply:
                                reply[stopareacode][tpc]['Stop'] = tpc_meta[tpc]
                        else:
    	    	            reply[stopareacode][tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
    	    	            tpc_store[tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
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
                label, name, subscription, path, endian, enc, res1, timestamp, _ = line[2:].split('|')
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
                if row['TripStopStatus'] != 'UNKNOWN' and row['TripStopStatus'] != 'PLANNED': #Keeps status of the dataowners supplying us data
                    last_updatestore['DataOwner'][row['DataOwnerCode']] = row['LastUpdateTimeStamp']
                    last_updatestore['Subscription'][subscription] = row['LastUpdateTimeStamp']
                elif row['DataOwnerCode'] not in last_updatestore['DataOwner']:
                    last_updatestore['DataOwner'][row['DataOwnerCode']] = 'ERROR'
                if subscription not in last_updatestore['Subscription']:
                    last_updatestore['Subscription'][subscription] = 'ERROR'
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
            reply = {'LastUpdateTimeStamps' : last_updatestore, 'ServerTime' : strftime("%Y-%m-%dT%H:%M:%SZ",gmtime())}
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
