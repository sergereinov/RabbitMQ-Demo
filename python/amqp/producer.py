'''
AMQP-producer wrapper.
'''

import pika

######################################################
## RabbitMQ producer (AMQP)

class Producer:
    def __init__(self, host, exchange_type, exchange_name):
        '''init producer'''
        self.host = host
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.open()

    def publish(self, msg, routing_key=''):
        '''publish message with routing_key'''
        bmsg = bytes(msg, "utf8")
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=bmsg)

    def open(self):
        '''(re-)open connection'''
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)
    
    def close(self):
        '''cleanup connection'''
        self.connection.close()

    def process_data_events(self):
        self.connection.process_data_events()
