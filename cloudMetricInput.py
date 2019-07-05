#!/usr/bin/env python
import sys
from mysql_CloudMetric import MySQLCloudMetric
from broker_rabbit import BrokerRabbitMQ
import argparse

def main(db_file):
    sqlite = MySQLCloudMetric()
    i = 0
    while(1):
        i = i +1
        print(i)
        rows = sqlite.select_metric_by_active(db_file)
        for row in rows:
            #'/home/pablo/sintetico.txt', 2, 'BrokerPabloNotebook', 'guest', 'guest', '172.17.0.2', '15672', 'client-req', 'RabbitMQ', 1
            #IP, Porta, user, senha
            broker = BrokerRabbitMQ(row[7],row[8],row[5],row[6])
            if(broker.ativo):
                print(row)
                topic = row[1]
                file_metric = row[0]
                try:
                    ref_arquivo = open(file_metric, 'r+')
                    message = ref_arquivo.readline()
                    #Zera o tamanho do arquivo
                    ref_arquivo.truncate(0)
                    ref_arquivo.close()
                    broker.publicar('Cloud Metric',topic,message)
                    broker.connection.close()
                except:
                    print("Erro ao ler o arquivo " + file_metric)
                    sqlite.update_metric(db_file)
                
            else:
                sqlite.update_metric(db_file)
                

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d",  help="Caminho do banco de dados",default="/home/pablo/github/CloudMetric/db.sqlite3")
    args = parser.parse_args()
    main(args.d) 