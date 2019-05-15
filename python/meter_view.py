#!/usr/bin/env python3
'''
'''

import tkinter as tk

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.patches import Polygon
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
import json, sqlite3

import sys
import queue
from amqp.consumer_thread import ConsumerThread
from config import Config

######################################################
## config

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    ROUTE_KEY_FACILITY = 'meter'
    AFTER_UPD_EXCHANGE_TYPE = 'topic'
    AFTER_UPD_EXCHANGE_NAME = 'meters_db_updates'

######################################################
## DB part

class DB:

    # parametric view-like query prefix
    WITH_VIEW = '''
        WITH
        input(sel_id, d1, d2) AS ( SELECT :id, :d1, :d2 ),
        prev_d(m_id, prev_d) AS (
            SELECT meter_id, max([datetime]) FROM Metering, input
            WHERE [datetime] < input.d1
            GROUP BY meter_id
        ),
        vals AS (
            SELECT meter_id,
                CASE WHEN [datetime] < d1 THEN d1 ELSE [datetime] END as dt,
                value, [state]
                , input.d1, input.d2, prev_d.prev_d
            FROM Metering
            CROSS JOIN input
            LEFT JOIN prev_d ON m_id = meter_id
            WHERE (input.sel_id IS NULL OR meter_id = input.sel_id)
                AND (prev_d.prev_d IS NULL OR prev_d.prev_d <= [datetime])
                AND [datetime] <= input.d2
        )
        '''

    def __init__(self):
        self.db = None

    def open(self):
        self.db = sqlite3.connect('./storage.sqlite_db')

    def close(self):
        self.db.close()

    def get_known_meters(self):
        c = self.db.cursor()
        c.execute('SELECT DISTINCT meter_id FROM Metering')
        rows = c.fetchall()
        return list(map(lambda r: r[0], rows))
    
    def get_values(self, meter_id, t1, t2):
        d1 = t1.strftime('%Y-%m-%dT%H:%M:%S')
        d2 = t2.strftime('%Y-%m-%dT%H:%M:%S')
        c = self.db.cursor()
        c.execute(self.WITH_VIEW + 'SELECT dt,value FROM vals ORDER BY dt',
            {'id': meter_id, 'd1': d1, 'd2': d2})
        rows = c.fetchall()
        return list(map(
            lambda r: (datetime.strptime(r[0],'%Y-%m-%dT%H:%M:%S'),r[1])
            , rows))

######################################################
## GUI part

class ValuesFrame(tk.Frame):
    MIN_VALUE = 0
    MAX_VALUE = 105
    def __init__(self, title, *args, **kwargs):
        self._title = title

        tk.Frame.__init__(self, *args, **kwargs)

        fig, self._ax = plt.subplots()
        self._ax.xaxis.set_tick_params(rotation=15, labelsize=8)
        self._ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        self._ax.xaxis.set_major_locator(mdates.SecondLocator(bysecond=[0,15,30,45]))
        self._ax.xaxis.set_minor_locator(mdates.SecondLocator(bysecond=[0,5,10,15,20,25,30,35,40,45,50,55]))
        self._ax.grid(True)

        self._canvas = FigureCanvasTkAgg(fig, self)
        self._canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.last_line = None
        self.poly = None
        self.update_timeline()
    
    def invalidate_viewport(self):
        t2 = datetime.today()
        t1 = t2 - timedelta(seconds=65)
        self._ax.set_xlim([t1,t2])
        self._ax.set_ylim([self.MIN_VALUE,self.MAX_VALUE])
        self._canvas.draw_idle()

    def update_timeline(self):
        now = datetime.today()

        '''extend last_line to the current time'''
        if self.last_line:
            xd = self.last_line.get_xdata()
            xd[-1] = now
            self.last_line.set_xdata(xd)

        '''extend the poly to the current time'''
        if self.poly:
            self.poly.xy[-3][0] = mdates.date2num(now)
            self.poly.xy[-2][0] = mdates.date2num(now)

        self.invalidate_viewport()
        self.after(1000, self.update_timeline)

    def draw_data(self, data):
        '''
        update frame with data
        where data elems are points [(datetime,value),]
        '''

        ''''convert input data to xy arrays'''
        tx = []
        tv = []
        last_v = None
        for x,v,*_ in data:
            if last_v != None:
                tx.append(x)
                tv.append(last_v)
            tx.append(x)
            tv.append(v)
            last_v = v

        '''append last point for the current time'''
        if tx and tv:
            tx.append(datetime.today())
            tv.append(last_v)

        '''clear old plots and patches'''
        self._ax.clear()
        self._ax.grid(True)
        [p.remove() for p in reversed(self._ax.patches)]

        if tx and tv:
            '''plot main lines'''
            *_, self.last_line = self._ax.plot_date(tx, tv, 'r-', xdate=True, lw=3)

            '''draw transparent polygon area'''
            verts = [
                (mdates.date2num(tx[0]), 0), 
                *zip(map(mdates.date2num, tx), tv), 
                (mdates.date2num(tx[-1]), 0)
                ]
            self.poly = Polygon(verts, alpha=0.2, facecolor='r', edgecolor='r')
            self._ax.add_patch(self.poly)
            self._ax.set_title(self._title)
        else:
            self._ax.set_title('No data for ' + self._title)


        '''update viewport'''
        self.invalidate_viewport()

######################################################
# main
selected_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0

selected_id = 2 #test

# create AMQP-consumer
msg_queue = queue.Queue()
consumer_thread = ConsumerThread(
    msg_queue, 
    Cfg.AMQP_HOST, 
    Cfg.AFTER_UPD_EXCHANGE_TYPE, 
    Cfg.AFTER_UPD_EXCHANGE_NAME,
    Cfg.ROUTE_KEY_FACILITY + '.*'
)
consumer_thread.start()

# open DB
db = DB()
db.open()

# create Tk window
root = tk.Tk()
root.title('Meter-View')
vf = ValuesFrame('Meter #{}'.format(selected_id), root)
vf.pack()

# define work data & procs
delta = timedelta(seconds=65)

def update_view():
    global selected_id, vf, delta
    now = datetime.today()
    v = db.get_values(selected_id, now-delta, now)
    vf.draw_data(v)

def process_queue():
    global root, msg_queue, selected_id
    while msg_queue.qsize():
        try:
            jmsg = msg_queue.get(0)
            msg = json.loads(jmsg)
            upd_id = int(msg.get('id', '0'))
            if upd_id == selected_id:
                update_view()
        except queue.Empty:
            pass
    root.after(200, process_queue)

# init graph & timer loop
update_view()
process_queue()

# run Tk mainloop
root.protocol('WM_DELETE_WINDOW', lambda: root.quit())
tk.mainloop()

#stop
consumer_thread.stop() # set stop-flag

#cleanup
db.close()
consumer_thread.join() # wait exit
