'''
AMQP-consumer wrapper with dedicated thread.
'''

import threading
import queue
import pika

######################################################
## RabbitMQ producer (AMQP)

class ConsumerThread(threading.Thread):
    def __init__(self, msg_queue, host, exchange_type, exchange_name, routing_key=''):
        '''init consumer thread'''
        super(ConsumerThread, self).__init__()
        self._is_interrupted = False
        self.msg_queue = msg_queue
        self.host = host
        self.exchange_type = exchange_type
        self.exchange_name = exchange_name
        self.routing_key = routing_key

    def stop(self):
        '''set stop-flag'''
        self._is_interrupted = True

    def run(self):
        '''thread proc'''
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
        result = channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.exchange_name, routing_key=self.routing_key, queue=queue_name)

        for message in channel.consume(queue_name, auto_ack=True, exclusive=True, inactivity_timeout=1):
            if self._is_interrupted:
                break
            if not message:
                continue

            #method, properties, body = message
            #print(method, properties, body)

            _, _, body = message
            if not body:
                continue

            msg = body.decode("utf8")
            self.msg_queue.put(msg)
        
        channel.cancel() # cancel consuimng
        connection.close()
