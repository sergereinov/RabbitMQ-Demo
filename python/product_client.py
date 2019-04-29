#!/usr/bin/env python3
'''
'''
import tkinter as tk
from tkinter import ttk
import sqlite3
import queue
from amqp.producer import Producer
from amqp.consumer_thread import ConsumerThread
from config import Config

######################################################
## params

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    EXCHANGE_TYPE = 'fanout'
    EXCHANGE_NAME = 'products_update'

######################################################
## DB part

#db = sqlite3.connect(':memory:')
db = sqlite3.connect('./storage.sqlite_db')
cursor = db.cursor()
cursor.executescript('''
CREATE TABLE IF NOT EXISTS Products (
       id INTEGER PRIMARY KEY,
       name TEXT NOT NULL UNIQUE,
       qty INTEGER DEFAULT 0,
       last_update TEXT DEFAULT (datetime())
);
''')
db.commit()

# #test
# r1 = cursor.execute('INSERT OR IGNORE INTO Products (name, qty) VALUES ("name1", 10)')
# print('affected rowcount: ', r1.rowcount)
# r1 = cursor.execute('INSERT OR IGNORE INTO Products (name, qty) VALUES ("name2", 20)')
# print('affected rowcount: ', r1.rowcount)

class ProductItem:
    @classmethod
    def Select(cls, id=None, name=None):
        global cursor
        if id:
            cursor.execute('SELECT id,name,qty,last_update FROM Products WHERE id=? LIMIT 1', (id,))
        elif name:
            cursor.execute('SELECT id,name,qty,last_update FROM Products WHERE name=? LIMIT 1', (name,))
        rows = cursor.fetchall()
        if len(rows) > 0:
            cls._row = rows[0]
            cls.id = cls._row[0]
            cls.name = cls._row[1]
            cls.qty = cls._row[2]
            cls.last_update = cls._row[3]
        return cls

    @staticmethod
    def New(name, qty):
        global cursor, db
        result = cursor.execute('INSERT OR IGNORE INTO Products (name, qty) VALUES (?,?)', (name, qty))
        db.commit()
        return result.rowcount

    @staticmethod
    def Update(id, last_update, set_name, set_qty):
        global cursor, db
        result = cursor.execute('''
            UPDATE OR IGNORE Products SET
                name = :name,
                qty = :qty,
                last_update = datetime()
            WHERE id = :id AND last_update = :upd_time
        ''', {'name': set_name, 'qty': set_qty, 'id': id, 'upd_time': last_update})
        db.commit()
        return result.rowcount

    @staticmethod
    def Delete(id, last_update):
        global cursor, db
        result = cursor.execute('DELETE FROM Products WHERE id = ? AND last_update = ?', (id, last_update))
        db.commit()
        return result.rowcount


# print('affected rowcount: ', ProductItem.New('name3', 30))
# print('ProductItem.Select(1):', ProductItem.Select(1).__dict__)
# print('affected rowcount: ', ProductItem.Update(1, ProductItem.Select(1).last_update, 'name 1', 30))
# print('affected rowcount: ', ProductItem.Delete(1, ProductItem.Select(1).last_update))


######################################################
## GUI part

class ProductFrame(tk.Frame):
    MODE_NONE = 0
    MODE_NEW = 1
    MODE_EDIT = 2

    def __init__(self, *args, **kwargs):
        '''init frame'''
        self.queue = kwargs.pop('queue', None)
        self.notify_update = kwargs.pop('notify_update', None)

        tk.Frame.__init__(self, *args, **kwargs)

        self.treeview = ttk.Treeview(self, height=6)
        self.treeview['columns'] = ['id', 'name', 'qty', 'last_update']
        self.treeview['show'] = 'headings'
        self.treeview.heading('id', text='ID')
        self.treeview.heading('name', text='Product Name')
        self.treeview.heading('qty', text='Stock Quantity')
        self.treeview.heading('last_update', text='Modified')
        self.treeview.column('id', minwidth=0, width=40, stretch=tk.NO, anchor=tk.CENTER)
        self.treeview.column('name', minwidth=0, width=200)
        self.treeview.column('qty', minwidth=0, width=100, stretch=tk.NO, anchor=tk.CENTER)
        self.treeview.column('last_update', minwidth=0, width=120, stretch=tk.NO)

        self.treeview.pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        self.view_buttons = ttk.Frame(self)
        ttk.Button(self.view_buttons, text='New', command=self.onNew).pack(side=tk.LEFT, fill=tk.X, expand=1)
        ttk.Button(self.view_buttons, text='Edit', command=self.onEdit).pack(side=tk.LEFT, fill=tk.X, expand=1)
        ttk.Button(self.view_buttons, text='Delete', command=self.onDetele).pack(side=tk.LEFT, fill=tk.X, expand=1)
        ttk.Button(self.view_buttons, text='Refresh', command=self.updateTreeview).pack(side=tk.LEFT, fill=tk.X, expand=1)

        self.view_buttons.pack(side=tk.TOP, fill=tk.X, expand=1)

        self.var_edit_name = tk.StringVar()
        self.var_edit_qty = tk.StringVar()

        self.group = ttk.LabelFrame(self, text='Item', padding=(5,5,5,5))
        ttk.Label(self.group, text='Name:').grid(row=0, column=0)
        self.edit_name = ttk.Entry(self.group, textvariable=self.var_edit_name, width=30)
        self.edit_name.grid(row=0, column=1)
        ttk.Label(self.group, text='Qty:').grid(row=1, column=0)
        ttk.Entry(self.group, textvariable=self.var_edit_qty, width=10).grid(row=1, column=1, sticky=tk.W)
        ttk.Label(self.group).grid(row=2, column=0)
        self.button_ok = ttk.Button(self.group, text='OK', command=self.onOk, width=10)
        self.button_ok.grid(row=3, column=0)
        ttk.Button(self.group, text='Cancel', command=self.onCancel, width=10).grid(row=3, column=1, sticky=tk.W)

        self.group.pack(side=tk.TOP, fill=tk.X, expand=1)

        self.error_text = ttk.Label(self, foreground='#e33')
        self.error_text.pack(side=tk.TOP, fill=tk.X, expand=1, anchor=tk.W)

        self.clearItem()

        self.updateTreeview()

        self.process_inbound()

    def process_inbound(self):
        '''process inbound messages queue'''
        if not self.queue.empty():
            with self.queue.mutex:
                self.queue.queue.clear()
            self.updateTreeview()
        self.after(200, self.process_inbound)

    def enableEdit(self, enable, suffix = None):
        for child in self.group.winfo_children():
            child.configure(state='enable' if enable else 'disable')
        if enable:
            self.edit_name.focus()
        if suffix:
            self.group.config(text='Item (%s)' % suffix)
        else:
            self.group.config(text='Item')

    def clearItem(self):
        self.var_edit_name.set('')
        self.var_edit_qty.set('')
        self.enableEdit(False)
        self.error_text.config(text='')
        self.mode = ProductFrame.MODE_NONE

    def errorItem(self, text):
        self.error_text.config(text=text)
        self.button_ok.config(state=tk.DISABLED)

    def reportUpdate(self):
        if self.notify_update:
            self.notify_update()

    def onNew(self):
        self.onCancel()
        self.enableEdit(True, 'New')
        self.mode = ProductFrame.MODE_NEW

    def onEdit(self):
        self.onCancel()
        selected = self.treeview.focus()
        if selected:
            item = self.treeview.item(selected)
            vals = item['values']
            self.item_id = vals[0]
            self.item_last_update = vals[3]
            self.var_edit_name.set(vals[1])
            self.var_edit_qty.set(vals[2])
            self.enableEdit(True, 'Edit')
            self.mode = ProductFrame.MODE_EDIT

    def onDetele(self):
        self.onCancel()
        selected = self.treeview.focus()
        if selected:
            item = self.treeview.item(selected)
            vals = item['values']
            rows_affected = ProductItem.Delete(vals[0], vals[3])
            if rows_affected > 0:
                self.clearItem() #ok
                self.reportUpdate()
            else:
                self.errorItem('No rows affected! No such item or last_update changed in background!')

        self.updateTreeview()

    def onOk(self):
        name = self.var_edit_name.get()
        qty = 0
        try:
            qty = int(self.var_edit_qty.get())
        except ValueError:
            pass

        rows_affected = 0

        if self.mode == ProductFrame.MODE_NEW:
            rows_affected = ProductItem.New(name, qty)
        elif self.mode == ProductFrame.MODE_EDIT:
            rows_affected = ProductItem.Update(self.item_id, self.item_last_update, name, qty)

        if rows_affected > 0:
            self.clearItem() #ok
            self.reportUpdate()
        else:
            self.errorItem('No rows affected! Name is not unique or last_update changed in background!')

        self.updateTreeview()

    def onCancel(self):
        self.clearItem()

    def updateTreeview(self):
        global cursor
        self.treeview.delete(*self.treeview.get_children())        
        cursor.execute('SELECT id,name,qty,last_update FROM Products ORDER BY name')
        rows = cursor.fetchall()
        index = 0
        for row in rows:
            self.treeview.insert("", index, row[0], values=row)
            index += 1

######################################################
## main

# init rabbit parts
msg_queue = queue.Queue()
consumer_thread = ConsumerThread(msg_queue, Cfg.AMQP_HOST, Cfg.EXCHANGE_TYPE, Cfg.EXCHANGE_NAME)
consumer_thread.start()
producer = Producer(Cfg.AMQP_HOST, Cfg.EXCHANGE_TYPE, Cfg.EXCHANGE_NAME)


root = tk.Tk()
root.title('Products')

ProductFrame(root, 
    queue = msg_queue,
    notify_update = lambda: producer.publish('upd')
    ).pack(fill=tk.BOTH, expand=1)

root.update() # completes any pending geometry 
root.minsize(root.winfo_width(), root.winfo_height()) # set minimum window size


def process_data_events():
    global root, producer
    producer.process_data_events()
    root.after(500, process_data_events)
process_data_events() #producer heartbit

root.protocol('WM_DELETE_WINDOW', lambda: root.quit())
tk.mainloop()  # GUI mainloop

# stop consume
consumer_thread.stop() # set stop-flag

# cleanup
producer.close()
consumer_thread.join() # wait exit


