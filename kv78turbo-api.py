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
import codecs
from threading import Thread
import simplejson as json

output = codecs.open('/var/ovapi/kv7.openov.nl/GOVI/CURRENTDB', 'r', 'UTF-8')
dbname = output.read().split('\n')[0]
conn = psycopg2.connect("dbname='%s'" % (dbname))

tpc_store = {}
stopareacode_store = {}
line_store = {}
journey_store = {}
last_updatestore = {'DataOwner' : {}, 'Subscription' : {}}
generalmessagestore = {}

tpc_meta = {}
line_meta = {}
destination_meta = {}

journeystoptypefiltered = False #Indicates whether journeystoptype was filtered during import KV7import 

print 'Start loading kv7 data'
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
if journeystoptypefiltered:
    cur.execute("""
SELECT timingpointcode,timingpointname,timingpointtown,stopareacode,ST_Y(the_geom)::NUMERIC(9,7) AS lat,ST_X(the_geom)::NUMERIC(8,7) AS lon
FROM (select *,ST_Transform(st_setsrid(st_makepoint(coalesce(locationx_ew,0), coalesce(locationy_ns,0)), 28992), 4326) AS the_geom FROM timingpoint) as t
""",[])
else:
    cur.execute("""
SELECT timingpointcode,timingpointname,timingpointtown,stopareacode,ST_Y(the_geom)::NUMERIC(9,7) AS lat,ST_X(the_geom)::NUMERIC(8,7) AS lon
FROM
(SELECT DISTINCT dataownercode,userstopcode FROM localservicegrouppasstime WHERE journeystoptype != 'INFOPOINT') as u
JOIN usertimingpoint as ut USING (dataownercode,userstopcode)
JOIN (select *,ST_Transform(st_setsrid(st_makepoint(coalesce(locationx_ew,0), coalesce(locationy_ns,0)), 28992), 4326) AS the_geom FROM timingpoint) as t USING (timingpointcode)
""",[])
kv7rows = cur.fetchall()
for kv7row in kv7rows:
    tpc_meta[intern(kv7row[0])] = {'TimingPointName' : intern(kv7row[1]), 'TimingPointTown' : intern(kv7row[2]), 'StopAreaCode' : kv7row[3], 'Latitude' : float(kv7row[4]), 'Longitude' : float(kv7row[5])}
    #if kv7row[6] != 'UNKNOWN' and kv7row[6] is not None:
    #    tpc_meta[kv7row[0]]['TimingPointVisualAccessible'] = kv7row[6]
    #if kv7row[7] != 'UNKNOWN' and kv7row[7] is not None:
    #    tpc_meta[kv7row[0]]['TimingPointWheelChairAccessible'] = kv7row[7]
cur.close()

cur = conn.cursor()
cur.execute("""
SELECT DISTINCT on (p.dataownercode,p.lineplanningnumber,p.linedirection)
p.dataownercode,p.lineplanningnumber,p.linedirection,p.destinationcode
FROM (
	SELECT distinct on (dataownercode,lineplanningnumber,linedirection)
	dataownercode,lineplanningnumber,linedirection,journeypatterncode from localservicegrouppasstime
	WHERE journeystoptype = 'LAST' 
	ORDER BY dataownercode,lineplanningnumber,linedirection,userstopordernumber DESC) as ljp,localservicegrouppasstime as p
WHERE
p.dataownercode = ljp.dataownercode AND 
p.lineplanningnumber = ljp.lineplanningnumber AND
p.linedirection = ljp.linedirection AND
p.journeypatterncode = ljp.journeypatterncode AND
p.journeystoptype = 'FIRST'
""")
kv7rows = cur.fetchall()
for kv7row in kv7rows:
    line_id = '_'.join([kv7row[0],kv7row[1],str(kv7row[2])])
    line_store[line_id] = {'Network': {}, 'Actuals': {}, 'Line' : {}}
    line_store[line_id]['Line'] = {'DataOwnerCode' : kv7row[0], 'LineDirection' : kv7row[2], 'LinePlanningNumber' : kv7row[1], 'DestinationCode' : kv7row[3]}
if 'ARR_16001_1' in line_store:
    line_store['ARR_16001_1']['Line']['DestinationCode'] = '3000'
cur.close()
conn.close()
print 'Loaded KV7 data'

try:
    f = open('generalmessage.json','r')
    generalmessagestore = json.load(f)
    f.close()
    for id,msg in generalmessagestore.items():
        if msg['TimingPointCode'] not in tpc_store:
            tpc_store[msg['TimingPointCode']] = {'Passes' : {}, 'GeneralMessages' : {}}
        tpc_store[msg['TimingPointCode']]['GeneralMessages'][id] = msg
except Exception as e:
    print e

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
    now = long((datetime.today() - timedelta(seconds=60)).strftime("%s"))
    for timingpointcode, values in tpc_store.items():
        for journey, row in values['Passes'].items():
            if now > row['ExpectedArrivalTime'] and now > row['ExpectedDepartureTime']:
                del(tpc_store[timingpointcode]['Passes'][journey])
		if row['TripStopStatus'] in ['PLANNED','UNKNOWN','CANCEL']:
                    userstoporder = row['UserStopOrderNumber']
                    if journey in journey_store and userstoporder in journey_store[journey]['Stops']:
                    	line_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber'] + '_' + str(row['LineDirection'])
    	                if line_id in line_store and journey in line_store[line_id]['Actuals']:
    	                    del(line_store[line_id]['Actuals'][journey])
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
    for id,row in generalmessagestore.items():
        if 'MessageEndTime' in row and now > row['MessageEndTime']:
            if row['TimingPointCode'] in tpc_store and id in tpc_store[row['TimingPointCode']]['GeneralMessages']:
                del(tpc_store[row['TimingPointCode']]['GeneralMessages'][id])
            del(generalmessagestore[id])	

def setDelayIncrease(oldrow,newrow):
    if newrow['TripStopStatus'] == 'DRIVING' and oldrow['TripStopStatus'] != 'PLANNED' and oldrow['JourneyStopType'] != 'LAST':
        targetdeparture = oldrow['TargetDepartureTime']
        timediff = (newrow['LastUpdateTimeStamp'] - oldrow['LastUpdateTimeStamp'])
        olddelay = (oldrow['ExpectedDepartureTime'] - targetdeparture)
        newdelay = (newrow['ExpectedDepartureTime'] - targetdeparture)
        delaydiff = (newdelay - olddelay)
        if delaydiff > timediff and newdelay > 60:
            oldrow['JourneyDisrupted'] = True
            return
    if 'JourneyDisrupted' in oldrow:
        del(oldrow['JourneyDisrupted'])

def storecurrect(newrow): 	    
    newrow['ExpectedArrivalTime'] = totimestamp(newrow['OperationDate'], newrow['ExpectedArrivalTime'], newrow)
    newrow['ExpectedDepartureTime'] = totimestamp(newrow['OperationDate'], newrow['ExpectedDepartureTime'], newrow)
    newrow['TargetArrivalTime'] = totimestamp(newrow['OperationDate'], newrow['TargetArrivalTime'], newrow)
    newrow['TargetDepartureTime'] = totimestamp(newrow['OperationDate'], newrow['TargetDepartureTime'], newrow)
    date,time = newrow['LastUpdateTimeStamp'].split('T')
    newrow['LastUpdateTimeStamp'] = totimestamp(date,time[:-6],None)

    id = '_'.join([newrow['DataOwnerCode'], str(newrow['LocalServiceLevelCode']), newrow['LinePlanningNumber'], str(newrow['JourneyNumber']), str(newrow['FortifyOrderNumber'])])
    if id in journey_store and int(newrow['UserStopOrderNumber']) in journey_store[id]['Stops']:
        row = journey_store[id]['Stops'][int(newrow['UserStopOrderNumber'])]
        if row['TripStopStatus'] == 'LINESTOPCANCEL':
            newrow['TripStopStatus'] = 'LINESTOPCANCEL'
        setDelayIncrease(row,newrow)
	if newrow['WheelChairAccessible'] == 'UNKNOWN':
	    newrow['WheelChairAccessible'] = row['WheelChairAccessible'] # Because of agencies not implementing the accessiblity tag in KV6, we're better off ignoring UKNOWNS unfortunately
        row.update(newrow)
    else:
        row = newrow

    line_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber'] + '_' + str(row['LineDirection'])
    linemeta_id = row['DataOwnerCode'] + '_' + row['LinePlanningNumber']
    destinationmeta_id = row['DataOwnerCode'] + '_' + row['DestinationCode']
    pass_id = '_'.join([row['UserStopCode'], str(row['UserStopOrderNumber'])])

    for x in ['JourneyPatternCode', 'JourneyNumber', 'FortifyOrderNumber', 'UserStopOrderNumber', 'NumberOfCoaches', 'LocalServiceLevelCode', 'LineDirection']:
        try:
            if x in row and row[x] is not None and row[x] != 'UNKNOWN':
	        row[x] = int(row[x])
	    else:
                del(row[x])
        except:
	    pass  
    row['IsTimingStop'] = (row['IsTimingStop'] == '1')

    if row['TimingPointCode'] not in tpc_store:
    	    tpc_store[row['TimingPointCode']] = {'Passes' : {id: row}, 'GeneralMessages' : {}}
    else:
    	    tpc_store[row['TimingPointCode']]['Passes'][id] = row

    if row['TimingPointCode'] in tpc_meta:
        stopareacode = tpc_meta[row['TimingPointCode']]['StopAreaCode']
        if stopareacode != None:
    	    if stopareacode not in stopareacode_store:
    	    	    stopareacode_store[stopareacode] = [row['TimingPointCode']]
    	    elif row['TimingPointCode'] not in stopareacode_store[stopareacode]:
    	    	    stopareacode_store[stopareacode].append(row['TimingPointCode'])
    
    if line_id not in line_store:
    	line_store[line_id] = {'Network': {}, 'Actuals': {}, 'Line' : {}}
    	line_store[line_id]['Line'] = {'DataOwnerCode' : row['DataOwnerCode'], 'LineDirection' : row['LineDirection'], 'LinePlanningNumber' : row['LinePlanningNumber'], 'DestinationCode' : row['DestinationCode']}
    if row['JourneyPatternCode'] not in line_store[line_id]['Network']:
        line_store[line_id]['Network'][row['JourneyPatternCode']] = {}
    if row['UserStopOrderNumber'] not in line_store[line_id]['Network'][row['JourneyPatternCode']]:
        line_store[line_id]['Network'][row['JourneyPatternCode']][row['UserStopOrderNumber']] = {
            'TimingPointCode': row['TimingPointCode'],
            'IsTimingStop': row['IsTimingStop'],
            'UserStopOrderNumber':row['UserStopOrderNumber']
            }
    if id not in journey_store:
    	journey_store[id] = {'Stops' : {row['UserStopOrderNumber']: row}}
        if row['WheelChairAccessible'] != 'UNKNOWN':
            line_store[line_id]['Line']['LineWheelchairAccessible'] = row['WheelChairAccessible']
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
    elif row['TripStopStatus'] == 'PLANNED' and row['TimingPointDataOwnerCode'] == 'ALGEMEEN' and id not in line_store[line_id]['Actuals'] and int(row['UserStopOrderNumber']) == 1: #add planned journeys
    	line_store[line_id]['Actuals'][id] = row
    elif (row['TripStopStatus'] == 'UNKNOWN' or row['TripStopStatus'] == 'CANCEL') and id in line_store[line_id]['Actuals']: #Delete canceled or non live journeys
	del(line_store[line_id]['Actuals'][id])
                   	
def savemsgstore():
    try:
        f = open('generalmessage.json','w')
        json.dump(generalmessagestore,f)
        f.close()
    except:
        pass
                   	
def storemessage(row):
    id = '_'.join([row['DataOwnerCode'], row['MessageCodeDate'], row['MessageCodeNumber'], row['TimingPointDataOwnerCode'], row['TimingPointCode']])
    if row['MessageEndTime'] is None or int(row['MessageEndTime'][0:4]) < 1900:
        return
    row['MessageEndTime'] = int(datetime.strptime(row['MessageEndTime'][:-6],"%Y-%m-%dT%H:%M:%S").strftime("%s"))
    row['MessageStartTime'] = int(datetime.strptime(row['MessageStartTime'][:-6],"%Y-%m-%dT%H:%M:%S").strftime("%s"))
    row['MessageTimeStamp'] = int(datetime.strptime(row['MessageTimeStamp'][:-6],"%Y-%m-%dT%H:%M:%S").strftime("%s"))
    if row['TimingPointCode'] in tpc_store:
        tpc_store[row['TimingPointCode']]['GeneralMessages'][id] = row
    else:
        tpc_store[row['TimingPointCode']] = {'Passes' : {}, 'GeneralMessages' : {id : row}}
    generalmessagestore[id] = row
    savemsgstore()

def deletemessage(row):
    id = '_'.join([row['DataOwnerCode'], row['MessageCodeDate'], row['MessageCodeNumber'], row['TimingPointDataOwnerCode'], row['TimingPointCode']])
    if row['TimingPointCode'] in tpc_store and id in tpc_store[row['TimingPointCode']]['GeneralMessages']:
         del(tpc_store[row['TimingPointCode']]['GeneralMessages'][id])
    if id in generalmessagestore:
         del(generalmessagestore[id])	
    savemsgstore()
	
        
context = zmq.Context()

kv8 = context.socket(zmq.SUB)
kv8.connect(ZMQ_KV8)
kv8.setsockopt(zmq.SUBSCRIBE, "/GOVI/KV8")

kv7 = context.socket(zmq.PULL)
kv7.bind(ZMQ_KV7)

poller = zmq.Poller()
poller.register(kv8, zmq.POLLIN)
poller.register(kv7, zmq.POLLIN)

garbage = 0

def addMeta(passtimes,terminating):
    result = {}
    for key, values in passtimes.items():
      if values['JourneyStopType'] != 'LAST' or terminating:
        result[key] = values.copy()
        linemeta_id = values['DataOwnerCode'] + '_' + values['LinePlanningNumber']
        if linemeta_id in line_meta:
            result[key].update(line_meta[linemeta_id])
        destinationmeta_id = values['DataOwnerCode'] + '_' + values['DestinationCode']
        if destinationmeta_id in destination_meta:
            result[key]['DestinationName50'] = destination_meta[destinationmeta_id]
        timingpointcode = values['TimingPointCode']
        if timingpointcode in tpc_meta:
            result[key].update(tpc_meta[timingpointcode])
        result[key]['ExpectedDepartureTime'] = todate(result[key]['ExpectedDepartureTime'])
        result[key]['ExpectedArrivalTime'] = todate(result[key]['ExpectedArrivalTime'])
        result[key]['TargetDepartureTime'] = todate(result[key]['TargetDepartureTime'])
        result[key]['TargetArrivalTime'] = todate(result[key]['TargetArrivalTime'])
        if result[key]['TripStopStatus'] == 'LINESTOPCANCEL':
            result[key]['TripStopStatus'] = 'CANCEL'
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
                        now = long((datetime.today()).strftime("%s"))
                        reply[tpc]['GeneralMessages'] = deepcopy(tpc_store[tpc]['GeneralMessages'])
                        for key,value in reply[tpc]['GeneralMessages'].items():
                            if value['MessageStartTime'] > now:
                                del(reply[tpc]['GeneralMessages'][key])
                            else:
                                if 'MessageEndTime' in value:
                                    value['MessageEndTime'] = todate(value['MessageEndTime'])
                                value['MessageStartTime'] = todate(value['MessageStartTime'])
                                value['MessageTimeStamp'] = todate(value['MessageTimeStamp'])
                        reply[tpc]['Passes'] = addMeta(reply[tpc]['Passes'],(arguments[-1] != 'departures'))
                if tpc in tpc_meta and tpc != '':
                    if tpc in reply:
                        reply[tpc]['Stop'] = tpc_meta[tpc]
			reply[tpc]['Stop']['TimingPointCode'] = tpc
                    else:
    	    	        reply[tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
    	    	        tpc_store[tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
			reply[tpc]['Stop']['TimingPointCode'] = tpc
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
                    reply[journey]['ServerTime'] = strftime("%Y-%m-%dT%H:%M:%SZ",gmtime())
                    reply[journey]['Stops'] = addMeta(reply[journey]['Stops'],True)
        return reply

def queryStopAreas(arguments):
    if len(arguments) == 1:
        reply = {}
        for stopareacode in stopareacode_store:
            for tpc in stopareacode_store[stopareacode]:
                if tpc in tpc_meta:
                    reply[stopareacode] = tpc_meta[tpc].copy()
		    if 'TimingPointWheelChairAccessible' in reply[stopareacode]:
		        del(reply[stopareacode]['TimingPointWheelChairAccessible'])
		    if 'TimingPointVisualAccessible' in reply[stopareacode]:
		        del(reply[stopareacode]['TimingPointVisualAccessible'])
        return reply
    else:
        reply = {} 
        for stopareacode in set(arguments[1].split(',')):
            if stopareacode in stopareacode_store and stopareacode != '':
                reply[stopareacode] = {}
                reply[stopareacode]['ServerTime'] = strftime("%Y-%m-%dT%H:%M:%SZ",gmtime())
                for tpc in stopareacode_store[stopareacode]:
                    if tpc in tpc_store and tpc != '':
                        reply[stopareacode][tpc] = tpc_store[tpc].copy()
                        reply[stopareacode][tpc]['GeneralMessages'] = deepcopy(tpc_store[tpc]['GeneralMessages'])
                        now = long((datetime.today()).strftime("%s"))
                        for key,value in reply[stopareacode][tpc]['GeneralMessages'].items():
                            if value['MessageStartTime'] > now:
                                del(reply[stopareacode][tpc]['GeneralMessages'][key])
                            else:
                                if 'MessageEndTime' in value:
                                    value['MessageEndTime'] = todate(value['MessageEndTime'])
                                value['MessageStartTime'] = todate(value['MessageStartTime'])
                                value['MessageTimeStamp'] = todate(value['MessageTimeStamp'])
                        reply[stopareacode][tpc]['Passes'] = addMeta(reply[stopareacode][tpc]['Passes'],(arguments[-1] != 'departures'))
                        if tpc in tpc_meta:
                            if tpc in reply:
                                reply[stopareacode][tpc]['Stop'] = tpc_meta[tpc]
                        else:
    	    	            reply[stopareacode][tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
    	    	            tpc_store[tpc] = {'Stop' : tpc_meta[tpc], 'GeneralMessages' : {}, 'Passes' : {}}
        return reply
   	
def queryLines(arguments,no_network=False):
    if len(arguments) == 1:
        reply = {}
        for line, values in line_store.items():
          if len(values['Network']) > 0:
            reply[line] = values['Line'].copy()
            linemeta_id = values['Line']['DataOwnerCode'] + '_' + values['Line']['LinePlanningNumber']
            if 'DestinationCode' in values['Line']:
                destination_id = values['Line']['DataOwnerCode']+'_'+values['Line']['DestinationCode']
                if destination_id in destination_meta:
                    reply[line]['DestinationName50'] = destination_meta[destination_id]
            if linemeta_id in line_meta:
                reply[line].update(line_meta[linemeta_id])
        return reply
    else:
        reply = {}
        for line in set(arguments[1].split(',')):
            if line in line_store and line != '':
                if len(line_store[line]['Network']) == 0:
                    continue
                reply[line] = deepcopy(line_store[line])
                reply[line]['ServerTime'] = strftime("%Y-%m-%dT%H:%M:%SZ",gmtime())
                reply[line]['Actuals'] = addMeta(reply[line]['Actuals'],True)
                line_id = reply[line]['Line']['DataOwnerCode']+'_'+reply[line]['Line']['LinePlanningNumber']
                if line_id in line_meta:
                    reply[line]['Line'].update(line_meta[line_id])
                if 'DestinationCode' in reply[line]['Line']:
                    destination_id = reply[line]['Line']['DataOwnerCode']+'_'+reply[line]['Line']['DestinationCode']
                    if destination_id in destination_meta:
                        reply[line]['Line']['DestinationName50'] = destination_meta[destination_id]
                if no_network:
                    del(reply[line]['Network'])
                else:
                    for journeypatterncode,journeypattern in reply[line]['Network'].items():
                        for userstoporder, timingpoint in journeypattern.items():
                            if timingpoint['TimingPointCode'] in tpc_meta:
                                timingpoint.update(tpc_meta[timingpoint['TimingPointCode']])
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
                    row[k] = None
                else:              
                    row[k] = v
            for x in ['ReasonType', 'AdviceType', 'AdviceContent','SubAdviceType','MessageType','ReasonContent','OperatorCode', 'SubReasonType', 'MessageContent']:
                if x in row and row[x] is None:
                    del(row[x])
            if type == 'DATEDPASSTIME':
                if 'SideCode' in row and row['SideCode'] in ['-','Left','Right']:
                    del(row['SideCode'])
                elif 'SideCode' in row:
                    row['SideCode'] = intern(row['SideCode'])
                if row['TripStopStatus'] != 'UNKNOWN' and row['TripStopStatus'] != 'PLANNED': #Keeps status of the dataowners supplying us data
                    last_updatestore['DataOwner'][row['DataOwnerCode']] = row['LastUpdateTimeStamp']
                    last_updatestore['Subscription'][subscription] = row['LastUpdateTimeStamp']
                elif row['DataOwnerCode'] not in last_updatestore['DataOwner']:
                    last_updatestore['DataOwner'][row['DataOwnerCode']] = 'ERROR'
                if subscription not in last_updatestore['Subscription']:
                    last_updatestore['Subscription'][subscription] = 'ERROR'
                if row['JourneyStopType'] != 'INFOPOINT':
                    storecurrect(row)
            elif type == 'GENERALMESSAGEUPDATE':
                print 'GENERAL MESSAGE UPDATE'
                storemessage(row)
		print content
            elif type == 'GENERALMESSAGEDELETE':
                print 'GENERAL MESSAGE DELETE'
                deletemessage(row)
		print content
            else:
                print 'UNKNOWN TYPE : !!!!!' +  type
                print content

class read(Thread):
   def __init__ (self):
      Thread.__init__(self)
   def run(self):
      client = context.socket(zmq.REP)
      client.bind(ZMQ_KV78UWSGI)
      while True:
      	  url = client.recv()
          try:
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
                  reply = queryLines(arguments,no_network=(arguments[-1] == 'actuals'))
                  client.send_json(reply)
              elif arguments[0] == 'lastupdate':
                  reply = {'LastUpdateTimeStamps' : last_updatestore, 'ServerTime' : strftime("%Y-%m-%dT%H:%M:%SZ",gmtime())}
                  client.send_json(reply)            
              elif arguments[0] == 'generalmessage':
                  client.send_json(generalmessagestore)
              elif arguments[0] == 'admin':
                  client.send_json(['YO'])                
              else:
                  client.send_json([])
          except Exception as e:
              client.send_json([])
              print e
      Thread.__init__(self)


thread = read()
thread.start()

while True:
    socks = dict(poller.poll())
    if socks.get(kv8) == zmq.POLLIN:
        multipart = kv8.recv_multipart()
        content = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
        recvPackage(content)
    elif socks.get(kv7) == zmq.POLLIN:
    	data = kv7.recv_json()
        try:
            if 'PASSTIMES' in data:
                for pass_id, row in data['PASSTIMES'].items():
                    id = '_'.join([row['DataOwnerCode'], str(row['LocalServiceLevelCode']), row['LinePlanningNumber'], str(row['JourneyNumber']), str(row['FortifyOrderNumber'])])
                    if id not in journey_store or int(row['UserStopOrderNumber']) not in journey_store[id]['Stops']:
                        try:
        	            storecurrect(row)
                        except Exception as e:
                            print e
            if 'DESTINATION' in data:
                for dest_id, dest in data['DESTINATION'].items():
                    destination_meta[dest_id] = dest
            if 'TIMINGPOINT' in data:
                for tpc, timingpoint in data['TIMINGPOINT'].items():
                    tpc_meta[tpc] = timingpoint
            if 'LINE' in data:
                for line_id, line in data['LINE'].items():
                    line_meta[line_id] = line
            if 'NETWORK' in data:
                for line_id,network in data['NETWORK'].items():
                    newnetwork = {}
                    for jpcode,jp in network.items():
                        if jpcode not in newnetwork:
                            newnetwork[jpcode] = {}
                    for stoporder,stop in jp.items():
                        newnetwork[jpcode][int(stoporder)] = stop
                if line_id not in line_store:
                    line_store[line_id] = {'Network': {}, 'Actuals': {}, 'Line' : {}}
                line_store[line_id]['Network'] = newnetwork  
            if 'LINEMETA' in data:
                for line_id, meta in data['LINEMETA'].items():
                    if line_id in line_store:
                        line_store[line_id]['Line']['DataOwnerCode'] = meta['DataOwnerCode']
                        line_store[line_id]['Line']['LineDirection'] = meta['LineDirection']
                        line_store[line_id]['Line']['LinePlanningNumber'] = meta['LinePlanningNumber']
                        line_store[line_id]['Line']['DestinationCode'] = meta['DestinationCode']
        except Exception as e:
            print e
    if garbage > 200:
        cleanup()
        garbage = 0
    else:
        garbage += 1
