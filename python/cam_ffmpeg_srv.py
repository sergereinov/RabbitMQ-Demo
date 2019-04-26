#!/usr/bin/env python3
'''
ffmpeg server
'''

import pika
import signal, json
import subprocess, time
from config import Config

######################################################
## params

class Cfg:
    AMQP_HOST = Config.AMQP_HOST
    TASKS_EXCHANGE_TYPE = 'topic'
    TASKS_EXCHANGE_NAME = 'cam_tasks'
    TASKS_ROUTE_KEY_FACILITY = 'cam.task'
    TASKS_WORKERS_QUEUE = 'cam_tasks_queue'
    STOP_EXCHANGE_TYPE = 'fanout'
    STOP_EXCHANGE_NAME = 'cam_stop'

cmd_template = 'ffmpeg -i %s -an -vcodec copy -y -v quiet %s'
#cmd_template = 'ffmpeg -i %s -an -vcodec copy -y %s'
#cmd = cmd_template % (source_uri, filename.avi)

class CamHandler:
    def __init__(self, cam_id, source_uri):
        self.cam_id = cam_id
        self.source_uri = source_uri
        dt = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime())
        self.filename = 'video_id_%d_%s.mp4' % (cam_id, dt)
        self.ffmpeg = None

    def start(self):
        global cmd_template
        if self.ffmpeg is None or self.ffmpeg.poll() is not None:
            cmd = cmd_template % (self.source_uri, self.filename)
            print('Start cam_id', self.cam_id, ', cmd =', cmd)
            self.ffmpeg = subprocess.Popen(cmd, stdin=subprocess.PIPE, universal_newlines=True)

    def stop(self):
        if self.ffmpeg is not None and self.ffmpeg.poll() is None:
            try:
                self.ffmpeg.communicate(input='q', timeout=1)
                print('Stop cam_id', self.cam_id, 'ok')
            except subprocess.TimeoutExpired:
                print('Cant stop ffmpeg. Kill it.')
                self.ffmpeg.kill()

cameras = {}

######################################################
# work callbacks
def callback_start_task(ch, method, properties, body):    
    '''consumer callback_start_task'''
    global cameras

    print(" [x] start %r:%r" % (method.routing_key, body))

    #extract message
    jmsg = body.decode("utf8")
    msg = json.loads(jmsg)
    cam_id, cam_source_url = msg

    # Note: with the help of some global atomics make sure that the process is not yet running

    #register cam in local dictionary
    if cam_id in cameras:
        pass
    else:
        cameras[cam_id] = CamHandler(cam_id, cam_source_url)

    #start ffmpeg
    cameras[cam_id].start()

    # ACK for meter's message
    ch.basic_ack(delivery_tag=method.delivery_tag)    

def callback_stop_task(ch, method, properties, body):
    '''consumer callback_stop_task'''
    global cameras

    print(" [x] stop %r:%r" % (method.routing_key, body))

    #extract message
    jmsg = body.decode("utf8")
    msg = json.loads(jmsg)
    cam_id, _ = msg

    #find & stop ffmpeg
    if cam_id in cameras:
        cameras[cam_id].stop()
        del cameras[cam_id]

    # ACK for meter's message
    ch.basic_ack(delivery_tag=method.delivery_tag)

######################################################
# RabbitMQ part - create consumer worker

# create connection & channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host=Cfg.AMQP_HOST))
channel = connection.channel()

# declare tasks exchange, declare & bind round robin workers queue
channel.exchange_declare(exchange=Cfg.TASKS_EXCHANGE_NAME, exchange_type=Cfg.TASKS_EXCHANGE_TYPE)
channel.queue_declare(Cfg.TASKS_WORKERS_QUEUE)
channel.queue_bind(exchange=Cfg.TASKS_EXCHANGE_NAME, queue=Cfg.TASKS_WORKERS_QUEUE,
    routing_key=Cfg.TASKS_ROUTE_KEY_FACILITY + '.#')

# declare stop-signal fanout exchange, declare & bind anonymous exclusive queue
channel.exchange_declare(exchange=Cfg.STOP_EXCHANGE_NAME, exchange_type=Cfg.STOP_EXCHANGE_TYPE)
queue_name = channel.queue_declare('', exclusive=True).method.queue
channel.queue_bind(exchange=Cfg.STOP_EXCHANGE_NAME, queue=queue_name)

# configure consuming ops
channel.basic_qos(prefetch_count=1) #enable long ops workers selecting in round robin
channel.basic_consume(queue=Cfg.TASKS_WORKERS_QUEUE, on_message_callback=callback_start_task) # round robin
channel.basic_consume(queue=queue_name, on_message_callback=callback_stop_task) # exclusive anonymous

def ctrl_c_handler(signum, frame):
    channel.stop_consuming() # gracefully stopping
signal.signal(signal.SIGINT, ctrl_c_handler)

print(' [*] Worker started. Waiting for camera tasks. To exit press CTRL+C')

#run worker
channel.start_consuming()

print(' [*] Worker stopped.')

#cleanup
connection.close() # close pika connection
