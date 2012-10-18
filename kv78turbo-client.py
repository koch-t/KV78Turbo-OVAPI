import uwsgi
import zmq
from const import ZMQ_KV78UWSGI
import simplejson
import time

COMMON_HEADERS = [('Content-Type', 'application/json'), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Requested-With,Content-Type')]

lines = {}
stopareas = {}

def notfound(start_response):
    start_response('404 File Not Found', COMMON_HEADERS + [('Content-length', '2')])
    yield '[]'

def KV78Client(environ, start_response):
    url = environ['PATH_INFO'][1:]
    if len(url) > 0 and url[-1] == '/':
        url = url[:-1]
   
    arguments = url.split('/')
    if arguments[0] not in set(['tpc','journey', 'line','linesgh', 'stopareacode', 'lastupdate', 'generalmessage']) or len(arguments) > 3:
         return notfound(start_response)
   
    context = zmq.Context()
    client = context.socket(zmq.REQ)
    client.connect(ZMQ_KV78UWSGI)
    if arguments[0] == 'linesgh':
        if 'sgh' not in lines or (lines['sghtime'] < (time.time() - 1000)):
            client.send('line')
            data  = client.recv_json()
	    sghlines = {}
	    for key,value in data.items():
	       if key.startswith('HTM'):
	           sghlines[key] = value
	       elif key.startswith('CXX_R'):
                   sghlines[key] = value
	       elif key.startswith('VTN_40'):  
	           sghlines[key] = value
	       elif key.startswith('CXX_W'):
	           sghlines[key] = value 
               elif key.startswith('QBUZZ_r270') or key.startswith('QBUZZ_r170'):
                   sghlines[key] = value
            lines['sgh'] = simplejson.dumps(sghlines)
 	    lines['sghtime'] = time.time()
        reply = lines['sgh']
    elif len(arguments) == 1 and arguments[0] == 'line':
        if 'data' not in lines or (lines['time'] < (time.time() - 1000)):
	    client.send(url)
	    lines['data'] = client.recv()
	    lines['time'] = time.time()
	reply = lines['data']
    elif (len(arguments) == 1 and arguments[0] == 'stopareacode'):
        if 'data' not in stopareas or stopareas['time'] < time.time() - 1000:
	    client.send(url)
	    stopareas['data'] = client.recv()
	    stopareas['time'] = time.time()
	reply = stopareas['data']
    else:
        client.send(url)
        reply = client.recv()
    if len(reply) < 3:
        return notfound(start_response)
        
    start_response('200 OK', COMMON_HEADERS + [('Content-length', str(len(reply)))])
    return reply

uwsgi.applications = {'': KV78Client}
