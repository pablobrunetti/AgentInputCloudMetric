#!/usr/bin/env python
import sys
import pika.exceptions as exceptions
import pika
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
    
    
    while(True):
        broker = BrokerRabbitMQ('10.61.1.34','5672','guest','guest')
        topic = 'error_error'
        message = 'Transferindo'
        broker.publicar('Cloud Metric',topic,message)
        #broker.connection.close()
    