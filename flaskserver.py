from flask import Flask
from flask import request
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

#kafka =  KafkaClient("localhost:9101")
#producer = SimpleProducer(kafka)

app = Flask(__name__)

"""
run the code: python flaskserver.py
Call this by pasting this on a browser and hitting enter
http://10.25.36.140:10245/v1/dockertest
http://localhost:31005/v1/dockertest
"""

@app.route('/v1/ror', methods=['GET', 'POST'])
def seam_realtime_ror():
    acct_id = str(request.args.get('acct_id'))
    kafka =  KafkaClient("localhost:9101")
    producer = SimpleProducer(kafka)
    producer.send_messages('storm-sentence',acct_id)
    return "Request Processed! Account ID: " + acct_id

@app.route('/v1/ror/static', methods=['GET', 'POST'])
def static_ror():
    acct_id = request.args.get('acct_id')
    producer.send_messages('storm-sentence','1009725784,300,0,680,0,0,4,0,1')
    print "Hello - Who is there? asked Docker"
    return acct_id

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=8888,threaded=True)
