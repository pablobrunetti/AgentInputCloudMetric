#!/usr/bin/env python
import sys

class BrokerRabbitMQ():
    def __init__(self, host,port,username,password):
        try:
            credentials = pika.PlainCredentials(username, password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host,port=port,credentials=credentials,socket_timeout=1))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange='CloudMetric', exchange_type='topic')
            self.ativo = True
            print("Conexao efetuada com sucesso")
        except:
            print("Problema ao se conectar ao broker")
            self.ativo = False
    def publicar(self,exchange,topic,message):
        self.channel.basic_publish(
            exchange=exchange, routing_key=topic, body=message)
        print(" [x] Sent %r:%r" % (topic, message))
        