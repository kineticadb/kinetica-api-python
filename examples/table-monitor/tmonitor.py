import gpudb
import random
import zmq

#substitute ip address
ip_addr = '127.0.0.1'
#default db port substitute if necessary
db_port = '9191'
#default tm port
tm_port = '9002'

#connect to db and grab a handle to desired table 
db_conn = 'http://{0}:{1}'.format(ip_addr, db_port)
table_name = 'test'
db = gpudb.GPUdb(db_conn)
table_handle = gpudb.GPUdbTable(name=table_name,
                                db=db)

# Create monitor on a given table, returns topic id to subscribe to
table_monitor = table_handle.create_table_monitor()
source_topic = table_monitor.topic_id
print('Table name: {0}'.format(table_name))
print('Table monitor topic: {0}'.format(source_topic))

# Create table monitor subscription connection using topic id
tm_conn_str = "tcp://{0}:{1}".format(ip_addr,tm_port)
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect(tm_conn_str)
socket.setsockopt_string(zmq.SUBSCRIBE, source_topic)


while True:
    #blocking listen - recieves full payload per insertion and returns
    mpr = socket.recv_multipart()

    #decode the payload to records 
    chunk = gpudb.GPUdbRecord.decode_binary_data(table_handle.get_table_type().schema_string, mpr[1:])
    print(chunk)

    #underlying socket implementation 
    #will buffer incoming messages so none are lost during process portion

