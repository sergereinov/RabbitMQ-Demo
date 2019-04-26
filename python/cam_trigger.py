#!/usr/bin/env python3
'''
'''

import tkinter as tk
import sys, os, queue, json
import pika
from config import Config

######################################################
## params

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    TASKS_EXCHANGE_TYPE = 'topic'
    TASKS_EXCHANGE_NAME = 'cam_tasks'
    TASKS_ROUTE_KEY_FACILITY = 'cam.task'
    STOP_EXCHANGE_TYPE = 'fanout'
    STOP_EXCHANGE_NAME = 'cam_stop'

    DEFAULT_URI = 'rtsp://@192.168.21.166:8080/h264.sdp'

######################################################
## GUI part

class CamTriggerFrame(tk.Frame):
    def __init__(self, *args, **kwargs):
        '''init frame'''
        self.default_font = kwargs.pop('font', ('Arial', 12))
        self.source_uri_var = kwargs.pop('source_uri_var', None)
        command_start = kwargs.pop('command_start', None)
        command_stop = kwargs.pop('command_stop', None)        

        tk.Frame.__init__(self, *args, **kwargs)

        self.label = tk.Label(self, text='Source URI:', font=self.default_font, anchor=tk.W)
        self.edit = tk.Entry(self, width=40, font=self.default_font, textvariable=self.source_uri_var)

        self.buttons_frame = tk.Frame(self)
        self.button_start = tk.Button(self.buttons_frame, text='Start', font=self.default_font,
            command=command_start)
        self.button_stop = tk.Button(self.buttons_frame, text='Stop', font=self.default_font,
            command=command_stop)

        self.button_start.pack(side=tk.LEFT)
        tk.Frame(self.buttons_frame).pack(side=tk.LEFT, fill=tk.X, expand=1)
        self.button_stop.pack(side=tk.LEFT)

        self.label.pack(side=tk.TOP, expand=1, fill=tk.X)
        self.edit.pack(side=tk.TOP, expand=1, fill=tk.X, ipady=2)
        tk.Frame(self).pack(side=tk.TOP, pady=2)
        self.buttons_frame.pack(side=tk.TOP, fill=tk.X, expand=1)


######################################################
# RabbitMQ part

connection = pika.BlockingConnection(pika.ConnectionParameters(host=Cfg.AMQP_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=Cfg.TASKS_EXCHANGE_NAME, exchange_type=Cfg.TASKS_EXCHANGE_TYPE)
channel.exchange_declare(exchange=Cfg.STOP_EXCHANGE_NAME, exchange_type=Cfg.STOP_EXCHANGE_TYPE)

def publish(exname, rkey, cam_id, source_uri):
    global channel
    msg = (cam_id, source_uri)
    print(exname, msg)    
    jmsg = json.dumps(msg)
    bmsg = bytes(jmsg, "utf8")
    channel.basic_publish(exchange=exname, routing_key=rkey, body=bmsg)

######################################################
# main
cam_id = int(sys.argv[1]) if len(sys.argv) > 1 else os.getpid()
routing_key = Cfg.TASKS_ROUTE_KEY_FACILITY + '.' + str(cam_id)

root = tk.Tk()
root.title('Camera ID ' + str(cam_id))

source_uri_var = tk.StringVar() # linked tkinter string var
source_uri_var.set(Cfg.DEFAULT_URI)

CamTriggerFrame(
    root, font=('Arial',12),
    source_uri_var = source_uri_var,

    command_start = lambda:
        publish(Cfg.TASKS_EXCHANGE_NAME, routing_key, cam_id, source_uri_var.get()),

    command_stop = lambda:
        publish(Cfg.STOP_EXCHANGE_NAME, routing_key, cam_id, None),

    ).pack(fill=tk.BOTH, expand=1)

root.update() # completes any pending geometry 
root.minsize(root.winfo_width(), root.winfo_height()) # set minimum window size

# rabbit-heartbit
def process_data_events():
    connection.process_data_events()
    root.after(500, process_data_events)

process_data_events() 

root.protocol('WM_DELETE_WINDOW', lambda: root.quit())
tk.mainloop()  # GUI mainloop

#cleanup
connection.close()
