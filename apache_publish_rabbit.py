#!/usr/bin/env python
import sys
import pika.exceptions as exceptions
import pika
import json
import time
import datetime
import json

class BrokerRabbitMQ():
    def __init__(self, host,port,username,password):
        try:
            credentials = pika.PlainCredentials(username, password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host,port=port,credentials=credentials,socket_timeout=1))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange='CloudMetric', exchange_type='topic')
            self.ativo = True
            #print("Conexao efetuada com sucesso")
        except exceptions.ConnectionClosed as err:
            print(err) 
            self.ativo = False
    def publicar(self,exchange,topic,message):
        print(exchange)
        print(topic)
        self.channel.basic_publish(
            exchange=exchange, routing_key=topic, body=json.dumps(message))
        print(" [x] Sent %r:%r" % (topic, message))

if __name__ == "__main__":
    
    broker = BrokerRabbitMQ('10.65.1.8','5672','guest','guest')
    
    while(True):
        
        topic = 'LogApache_LogApache'
        timestamp  = datetime.datetime.utcnow().isoformat()
                             
        message = {'c77d0d8f-064c-4b24-9730-9bae4295e49b':{'LogApache':[{"timestamp": timestamp,"value": 5}]}}
        #print(message)
        broker.publicar('CloudMetric',topic,message)
        time.sleep(5)
    