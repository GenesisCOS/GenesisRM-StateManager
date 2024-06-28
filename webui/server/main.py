import threading 
from gevent.pywsgi import WSGIServer

from flask import Flask 
from flask_restful import Resource, Api 


webapp = Flask('WebUI')
webapi = Api(webapp)


@webapp.route('/')
def index():
    pass 


if __name__ == '__main__':
    # 启动 webserver
    __webserver = WSGIServer(('0.0.0.0', 8000), webapp)
    __webserver_thread = threading.Thread(
        target=__webserver.serve_forever,
        name='webserver'
    )
    __webserver_thread.start()
    __webserver_thread.join()