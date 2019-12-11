import gpudb
import random
import time

#substitute ip address
ip_addr = '127.0.0.1'
#default db port substitute if necessary
db_port = '9191'
db_conn = 'http://{0}:{1}'.format(ip_addr, db_port)
table_name = 'test'

db = gpudb.GPUdb(db_conn)

# Dummy sample table schema
table_schema = [
    ['animal', 'string', 'char32'],
    ['speed', 'double'],
    ['age', 'int', 'int8']
]

#if table exists drop and start again
if db.has_table(table_name=table_name)['table_exists']:
    status = db.clear_table(table_name=table_name)

table_handle = gpudb.GPUdbTable(_type=table_schema,
                                name=table_name,
                                db=db)

print('Table name: {0}'.format(table_name))

# Dummy sample values
animals = ['cow', 'horse', 'chicken']
while True:
    insertable = {'animal': random.choice(animals),
                  'speed': round(random.random(), 3),
                  'age': random.randint(1, 20)}

    table_handle.insert_records(insertable)
    print(insertable)
    time.sleep(1)

