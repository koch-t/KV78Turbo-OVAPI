import sys
import zmq
import simplejson as serializer
from datetime import datetime, timedelta
from time import strftime, strptime, gmtime
from gzip import GzipFile
from ctx import ctx
from gzip import GzipFile
from cStringIO import StringIO
import calendar

ZMQ_PUBSUB_NS = "tcp://83.98.158.170:6611"
ZMQ_KV7 = "tcp://127.0.0.1:6070"

# Initialize a zeromq CONTEXT
context = zmq.Context()
sys.stderr.write('Setting up a ZeroMQ SUB: %s\n' % (ZMQ_PUBSUB_NS))
subscribe_kv8 = context.socket(zmq.SUB)
subscribe_kv8.connect(ZMQ_PUBSUB_NS)
subscribe_kv8.setsockopt(zmq.SUBSCRIBE, '')

sys.stderr.write('Setting up a ZeroMQ PUSH: %s\n' % (ZMQ_KV7))
context = zmq.Context()
push = context.socket(zmq.PUSH)
push.connect(ZMQ_KV7)

# Set up a poller
poller = zmq.Poller()
poller.register(subscribe_kv8, zmq.POLLIN)

# Cache
actuals = {}

def productformula(treinsoort):
	if treinsoort == 'Stoptrein':
		return 25
	if treinsoort == 'Sneltrein':
		return 26
	if treinsoort == 'Intercity':
		return 27
	if treinsoort == 'Sprinter':
		return 28
	if treinsoort == 'Internationale trein':
		return 29
	if treinsoort == 'Fyra':
		return 30
	if treinsoort == 'ICE International':
		return 31
	if treinsoort == 'Thalys':
		return 32
	if treinsoort == 'CityNightLine':
		return 29
	if treinsoort == 'Stopbus i.p.v. Trein':
		return 18
	print treinsoort
	return 0

def dataownercode(vervoerder):
	if vervoerder == 'Veolia':
		return 'VTN'
	if vervoerder == 'Arriva':
		return 'ARR'
	if vervoerder == 'Connexxion':
		return 'CXX'
	if vervoerder == 'Syntus':
		return 'SYNTUS'
	return 'NS'

def linenumber(ritnummer):
        try:
                journeynumber = int(ritnummer)
        except:
                pass
                return 0
	if journeynumber > 0 and journeynumber < 999:
		return 10 * (journeynumber / 10)
	if journeynumber > 999 and journeynumber < 9999:
		return 100 * (journeynumber / 100)
	if journeynumber > 9999 and journeynumber < 99999:
		return 1000 * (journeynumber / 1000)
	if journeynumber > 99999 and journeynumber < 999999:
		return 10000 * (journeynumber / 10000)
	if journeynumber > 999999 and journeynumber < 9999999:
		return 100000 * (journeynumber / 100000)

def generateuserstopordernumber(targetdeparturetime):
        #as we dont have a slight, hack something up using the departuretme
        return calendar.timegm(targetdeparturetime.utctimetuple()) % 10000        

def convertkv(avt):
	row = {}
        try:
                row['JourneyNumber'] = avt['RitNummer']
        except:
                row['JourneyNumber'] = 0
	row['DestinationName50'] = avt['EindBestemming']
	row['DestinationCode'] = row['DestinationName50']
	row['TripStopStatus'] = 'DRIVING'
	if avt['Opmerkingen'] == 'Trein rijdt niet':
		row['TripStopStatus'] = 'CANCEL'
	else:
		row['MessageContent'] = avt['Opmerkingen']
	row['MessageContent'] = avt['Opmerkingen']
	row['AdviceContent'] = avt['ReisTip']
	row['SideCode'] = avt['VertrekSpoor']
	row['LastUpdateTimeStamp'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
	row['ProductFormulaType'] = productformula(avt['TreinSoort'])
	row['DataOwnerCode'] = dataownercode(avt['Vervoerder'])
        targetdeparturetime = datetime.strptime(avt['VertrekTijd'].split('+')[0], "%Y-%m-%d %H:%M:%S")
	expecteddeparturetime = targetdeparturetime + timedelta(minutes=int(avt['VertrekVertraging']))
	row['TargetDepartureTime'] = targetdeparturetime.strftime("%H:%M:%S")
        row['ExpectedDepartureTime'] = expecteddeparturetime.strftime("%H:%M:%S")
	row['TargetArrivalTime'] = row['TargetDepartureTime']
	row['ExpectedArrivalTime'] = row['ExpectedDepartureTime']
	row['TimingPointCode'] = avt['Station']
        row['IsTimingStop'] = '1'
	row['UserStopCode'] = row['TimingPointCode']
	row['StopAreaCode'] = row['TimingPointCode']
	row['LinePublicNumber'] = str(linenumber(row['JourneyNumber']))
	row['LinePlanningNumber'] = str(row['LinePublicNumber'])
	row['LineName'] =str(row ['LinePlanningNumber'])
	row['OperationDate'] = expecteddeparturetime.strftime("%Y-%m-%d")
	row['FortifyOrderNumber'] = '0'
        row['LocalServiceLevelCode'] = str(row['JourneyNumber'])
        row['NumberOfCoaches'] = '1'
	if (avt['TreinSoort'] == 'Stopbus i.p.v. Trein'):
		row['TransportType'] = 'BUS'
	else:
		row['TransportType'] = 'TRAIN'
        try:
                if row['JourneyNumber'] != None:
 	               if (int(row['JourneyNumber']) % 2 == 0):
		              row['LineDirection'] = '2'
	               else:
	 	              row['LineDirection'] = '1'
        except:
		row['LineDirection'] = '0'
	row['UserStopOrderNumber'] = str(generateuserstopordernumber(targetdeparturetime))
	return row
	
	

while True:
    socks = dict(poller.poll())

    if socks.get(subscribe_kv8) == zmq.POLLIN:
        print 'test'
        multipart = subscribe_kv8.recv_multipart()
        content = GzipFile('','r',0,StringIO(''.join(multipart[1:]))).read()
        c = ctx(content)
        print c.ctx
        if 'AVT' in c.ctx:
            rows = {}
            for row in c.ctx['AVT'].rows():
            	    row['Station'] = c.ctx['Subscription']
                    kv8 = convertkv(row)
            	    pass_id = '_'.join([kv8['DataOwnerCode'], str(kv8['LocalServiceLevelCode']), str(kv8['LinePlanningNumber']), str(kv8['JourneyNumber']), str(kv8['FortifyOrderNumber']), kv8['UserStopCode'], str(kv8['UserStopOrderNumber'])])
                    rows[pass_id] = kv8
                    print pass_id
            push.send_json(rows)
             
