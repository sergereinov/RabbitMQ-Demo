#!/usr/bin/env python3
'''
Meter emulator.
Emits measuring values into AMQP-exchange (Cfg.EXCHANGE_NAME).
Usage:
    python ./meter_emu.py [<meter_id>]
'''

import tkinter as tk
import sys, os, time, json, random
from amqp.producer import Producer
from config import Config

######################################################
## params

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    EXCHANGE_TYPE = 'topic'
    EXCHANGE_NAME = 'meters'
    ROUTE_KEY_FACILITY = 'meter'

class State:
    VALUE = 1
    ONLINE = 2
    OFFLINE = 3


######################################################
# main
random.seed(time.time())

meter_id = sys.argv[1] if len(sys.argv) > 1 else str(os.getpid())
initial_value = int(sys.argv[2]) if len(sys.argv) > 2 else random.randint(0, 100)

producer = Producer(Cfg.AMQP_HOST, Cfg.EXCHANGE_TYPE, Cfg.EXCHANGE_NAME)

root = tk.Tk()
root.title('Meter-Emu, ID: ' + meter_id)
root.geometry("180x160")

tk.Label(root, text="Meter ID: " + meter_id).pack(side=tk.TOP, pady=4, padx=4)

scale = tk.Scale(root, from_=100, to=0, tickinterval=25)
scale.pack(side=tk.TOP, fill=tk.Y, expand=1)
scale.set(initial_value)
last_value = -1

def meter_publish(msg):
    '''publish msg'''
    global producer, meter_id
    jmsg = json.dumps(msg)
    producer.publish(jmsg, Cfg.ROUTE_KEY_FACILITY + '.' + meter_id)

def meter_reading():
    '''read & publish the value'''
    global last_value, scale, meter_id
    value = scale.get()
    if last_value != value:
        msg = {'id':meter_id, 'ts':time.time(), 'value':value, 'state': State.VALUE}
        meter_publish(msg)
        last_value = value
    producer.process_data_events()
    root.after(1000, meter_reading)

#mark start
meter_publish({'id':meter_id, 'ts':time.time(), 'value':0, 'state': State.ONLINE})

#start poll
root.after(1100, meter_reading)

root.protocol('WM_DELETE_WINDOW', lambda: root.quit())
tk.mainloop()  # GUI mainloop

#mark stop
meter_publish({'id':meter_id, 'ts':time.time() + 1, 'value':0, 'state': State.OFFLINE})

# cleanup
producer.close()
