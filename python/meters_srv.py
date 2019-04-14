#!/usr/bin/env python3
'''
Meters server-worker (for horizontal scaling run multiple instances).

Consumes messages from meters (AMQP-exchange) and insert it into db.
'''

import pika
import sqlite3
import json
import signal
import time
from config import Config

######################################################
## params

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    EXCHANGE_TYPE = 'topic'
    EXCHANGE_NAME = 'meters'
    ROUTE_KEY_FACILITY = 'meter'
    WORKERS_QUEUE = 'meters_db_queue'
    AFTER_UPD_EXCHANGE_TYPE = 'topic'
    AFTER_UPD_EXCHANGE_NAME = 'meters_db_updates'

######################################################
# main

# DB part - connect to db and create tables if needed
# Sqlite3 is just for demo. You must use a real database server for real tasks.
# Do not overload the demo script.
# Sqlite can handle about 4-5 concurrency inserts per second.

#db = sqlite3.connect(':memory:')
db = sqlite3.connect('./storage.sqlite_db')
cursor = db.cursor()
cursor.executescript('''
CREATE TABLE IF NOT EXISTS Metering (
       id INTEGER PRIMARY KEY,
       meter_id INTEGER NOT NULL,
       datetime TEXT,
       value REAL,
       state INTEGER
);
''')
db.commit()

def on_message(meter_id, timestamp, value, state):
    '''Messages handler.
    Handles new message and return True if it decide to update DB
    :rtype: bool
    '''

    t = time.localtime(timestamp)
    dt = time.strftime('%Y-%m-%dT%H:%M:%S', t)
    
    cursor.execute('''
            INSERT INTO Metering (meter_id,datetime,value,state) VALUES(?,?,?,?) 
        ''', (meter_id, dt, value, state))
    db.commit()

    # in this simple scenario the database is always updated
    return True

# RabbitMQ part - create consumer worker
connection = pika.BlockingConnection(pika.ConnectionParameters(host=Cfg.AMQP_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=Cfg.EXCHANGE_NAME, exchange_type=Cfg.EXCHANGE_TYPE)
channel.queue_declare(Cfg.WORKERS_QUEUE, durable=True)
channel.queue_bind(exchange=Cfg.EXCHANGE_NAME, queue=Cfg.WORKERS_QUEUE,
    routing_key=Cfg.ROUTE_KEY_FACILITY + '.#')

# declare exchange for quick reports to all interested
channel.exchange_declare(exchange=Cfg.AFTER_UPD_EXCHANGE_NAME, exchange_type=Cfg.AFTER_UPD_EXCHANGE_TYPE)

print(' [*] Worker started. Waiting for meters messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    '''consumer callback'''
    print(" [x] %r:%r" % (method.routing_key, body))

    #extract message
    jmsg = body.decode("utf8")
    msg = json.loads(jmsg)
    meter_id = msg['id']

    try:

        #handle message
        if on_message(meter_id, msg['ts'], msg['value'], msg['state']):

            #report about DB update
            channel.basic_publish(
                exchange=Cfg.AFTER_UPD_EXCHANGE_NAME,
                routing_key=Cfg.ROUTE_KEY_FACILITY + '.' + meter_id,
                body='')

        # ACK for meter's message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except sqlite3.Error as e:
        print('sqlite3.Error:', e)
        # NAK - return message to original queue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


channel.basic_qos(prefetch_count=1) #enable long ops workers selecting in round robin
channel.basic_consume(queue=Cfg.WORKERS_QUEUE, on_message_callback=callback)

def sigint_handler(signum, frame):
    channel.stop_consuming() # gracefully stopping
 
signal.signal(signal.SIGINT, sigint_handler)

#run worker
channel.start_consuming()

print(' [*] Worker stopped.')

#cleanup
connection.close() # close pika connection
db.close() # close db connection
