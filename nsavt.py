import sys
import zmq
import simplejson as serializer
from datetime import datetime, timedelta
from time import strftime, strptime, gmtime
from gzip import GzipFile
from ctx import ctx
from gzip import GzipFile
from cStringIO import StringIO

ZMQ_PUBSUB_NS = "tcp://83.98.158.170:6611"

# Initialize a zeromq CONTEXT
context = zmq.Context()
sys.stderr.write('Setting up a ZeroMQ SUB: %s\n' % (ZMQ_PUBSUB_NS))
subscribe_kv8 = context.socket(zmq.SUB)
subscribe_kv8.connect(ZMQ_PUBSUB_NS)
subscribe_kv8.setsockopt(zmq.SUBSCRIBE, '')

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

def linenumber(journeynumber):
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
	return 0

def convertkv(avt):
	row = {}
        try:
                row['JourneyNumber'] = int(avt['RitNummer'])
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
	row['TargetDepartureTime'] = avt['VertrekTijd']
        targetdeparturetime = datetime.strptime(avt['VertrekTijd'].split('+')[0], "%Y-%m-%d %H:%M:%S")
	expecteddeparturetime = targetdeparturetime + timedelta(minutes=int(avt['VertrekVertraging']))
	row['ExpectedDepartureTime'] = expecteddeparturetime.strftime("%Y-%m-%d %H:%M:%S")
	row['TargetArrivalTime'] = row['TargetDepartureTime']
	row['ExpectedArrivalTime'] = row['ExpectedDepartureTime']
	row['TimingPointCode'] = avt['Station']
	row['UserStopCode'] = row['TimingPointCode']
	row['StopAreaCode'] = row['TimingPointCode']
	row['LinePublicNumber'] = linenumber(row['JourneyNumber'])
	row['LinePlanningNumber'] = row['LinePublicNumber']
	row['LineName'] = row ['LinePlanningNumber']
	row['OperationDate'] = expecteddeparturetime.strftime("%Y-%m-%d")
	row['FortifyOrderNumber'] = 0
	if (avt['TreinSoort'] == 'Stopbus i.p.v. Trein'):
		row['TransportType'] = 'BUS'
	else:
		row['TransportType'] = 'TRAIN'
	if (row['JourneyNumber'] % 2 == 0):
		row['LineDirection'] = '2'
	else:
		row['LineDirection'] = '1'
	row['UserStopOrderNumber'] = 0
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
            for row in c.ctx['AVT'].rows():
            	    row['Station'] = c.ctx['Subscription']
            	    print convertkv(row)

