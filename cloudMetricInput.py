#!/usr/bin/env python
import sys
from mysql_CloudMetric import MySQLCloudMetric
from broker_rabbit import BrokerRabbitMQ
import argparse
import json

def main(db_file):
    sqlite = MySQLCloudMetric()
    i = 0
    while(1):
        i = i +1
        #print(i)
        rows = sqlite.select_metric_by_active()
        for row in rows:
            #'/home/pablo/sintetico.txt', 2, 'BrokerPabloNotebook', 'guest', 'guest', '172.17.0.2', '15672', 'client-req', 'RabbitMQ', 1
            #IP, Porta, user, senha
            #print(row)
            #print(i)
            broker = BrokerRabbitMQ(row[7],row[8],row[5],row[6])
            
            if(broker.ativo):
                #print(row)
                topic = row[2]
                file_metric = row[1]
                #print(file_metric)
                try:
                    # read file
                    json_file = open(file_metric, 'r+')  
                    first = json_file.read(0)
                    #print('etapa 1')
                    #if(not first):
                    #    print('Arquivo vazio')
                    #    continue
                    message = json.load(json_file)
                    #print('etapa 1')
                    #print(data)
                    #print(str(data['Timestamp']))
                        
                    #prinnt('Passou')    
                    print(message)
                    #message = 'Transmitindo'
                    #Zera o tamanho do arquivo
                    json_file.truncate(0)
                    json_file.close()
                    broker.publicar('Cloud Metric',topic,message)
                    broker.connection.close()
                    #print('Saindoooo')
                    #return
                except:
                    continue
                    #print("Erro ao ler o arquivo " + file_metric)
                    #return
                    
                    #sqlite.update_metric(row[0])
                
            else:
                #return
                sqlite.update_metric(row[0])
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d",  help="Caminho do banco de dados",default="/home/pablo/github/CloudMetric/db.sqlite3")
    args = parser.parse_args()
    main(args.d) 