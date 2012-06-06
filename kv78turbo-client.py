import uwsgi
import zmq
from const import ZMQ_KV78UWSGI

COMMON_HEADERS = [('Content-Type', 'application/json'), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Requested-With,Content-Type')]

def notfound(start_response):
    start_response('404 File Not Found', COMMON_HEADERS + [('Content-length', '2')])
    yield '[]'

def KV78Client(environ, start_response):
    url = environ['PATH_INFO'][1:]
    if len(url) > 0 and url[-1] == '/':
        url = url[:-1]
        
    arguments = url.split('/')
    if arguments[0] not in set(['tpc', 'journey', 'line', 'stopareacode', 'lastupdate', 'generalmessage']) or len(arguments) > 2:
         return notfound(start_response)

    context = zmq.Context()
    client = context.socket(zmq.REQ)
    client.connect(ZMQ_KV78UWSGI)
    client.send(url)
    reply = client.recv()
    if len(reply) < 3:
        return notfound(start_response)
        
    start_response('200 OK', COMMON_HEADERS + [('Content-length', str(len(reply)))])
    return reply

uwsgi.applications = {'': KV78Client}
