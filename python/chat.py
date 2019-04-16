#!/usr/bin/env python3
'''
Simple chatting demo.
Emits and consume messages into/from AMQP-exchange (Cfg.EXCHANGE_NAME).
Usage:
    python ./chat.py [<user_name>]
'''

import tkinter as tk
import sys
import os
import queue

from amqp.producer import Producer
from amqp.consumer_thread import ConsumerThread
from config import Config

######################################################
## params

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    EXCHANGE_TYPE = 'topic'
    EXCHANGE_NAME = 'chat'


######################################################
## GUI part

class ChatFrame(tk.Frame):
    def __init__(self, *args, **kwargs):
        '''init chat frame'''
        self.default_font = kwargs.pop('font', ('Arial', 12))
        self.producer = kwargs.pop('producer', None)
        self.queue = kwargs.pop('queue', None)
        self.user_name = kwargs.pop('user_name', 'user_name')

        tk.Frame.__init__(self, *args, **kwargs)

        self.messages_frame = tk.Frame(self)

        self.listbox = tk.Listbox(self.messages_frame, font=self.default_font, height=15, width=50) #deflt h&w
        self.scroll = tk.Scrollbar(self.messages_frame, command=self.listbox.yview)
        self.listbox.config(yscrollcommand=self.scroll.set)
        self.listbox.pack(side=tk.LEFT, expand=1, fill=tk.BOTH)
        self.scroll.pack(side=tk.LEFT, fill=tk.Y)

        self.my_msg = tk.StringVar() # reactive tkinter string var
        self.edit = tk.Entry(self, font=self.default_font, textvariable=self.my_msg)
        self.edit.bind('<Return>', self.on_send)
        self.button_send = tk.Button(self, text='Send', font=self.default_font, command=self.on_send)

        self.messages_frame.pack(side=tk.TOP, expand=1, fill=tk.BOTH)
        self.edit.pack(side=tk.LEFT, expand=1, fill=tk.X)
        self.button_send.pack(side=tk.LEFT)

        self.edit.focus_set()
        self.process_inbound()

    def on_send(self, event=None):
        '''handle sending'''
        msg = self.my_msg.get()
        self.my_msg.set("")  # clears input
        pubmsg = self.user_name + "> " + msg
        self.producer.publish(pubmsg)
        # todo: reconnect on drop

    def process_inbound(self):
        '''process inbound messages queue'''
        while self.queue.qsize():
            try:
                msg = self.queue.get(0)
                self.listbox.insert(tk.END, msg)

                # clear selection
                sel = self.listbox.curselection()
                if (len(sel) > 0):
                    self.listbox.selection_clear(sel[0], sel[-1])
                
                # select last line
                self.listbox.selection_set(tk.END)
                self.listbox.see(tk.END)
            except queue.Empty:
                pass
        self.producer.process_data_events()
        self.after(200, self.process_inbound)


# main
user_name = sys.argv[1] if len(sys.argv) > 1 else 'username-' + str(os.getpid())

msg_queue = queue.Queue()

consumer_thread = ConsumerThread(msg_queue, Cfg.AMQP_HOST, Cfg.EXCHANGE_TYPE, Cfg.EXCHANGE_NAME)
consumer_thread.start()
producer = Producer(Cfg.AMQP_HOST, Cfg.EXCHANGE_TYPE, Cfg.EXCHANGE_NAME)

root = tk.Tk()
root.title('Chat (' + user_name + ')')

ChatFrame(
    root, font=('Arial',12),
    producer=producer, queue=msg_queue, user_name=user_name
    ).pack(fill=tk.BOTH, expand=1)

root.update() # completes any pending geometry 
root.minsize(root.winfo_width(), root.winfo_height()) # set minimum window size

# start chatting
producer.publish('{enter} ' + user_name) # echo user entered

root.protocol('WM_DELETE_WINDOW', lambda: root.quit())
tk.mainloop()  # GUI mainloop

# stop chatting
consumer_thread.stop() # set stop-flag
producer.publish('{quit} ' + user_name) # echo user quiting & also stimulate local consumer to check stop-flag

# cleanup
producer.close()
consumer_thread.join() # wait exit
