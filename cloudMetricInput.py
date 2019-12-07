#!/usr/bin/env python
import sys
from mysql_CloudMetric import MySQLCloudMetric
from broker_rabbit import BrokerRabbitMQ
import argparse
import json
import smbclient

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
                #print(topic)
                file_metric = row[1]
                
                #print(file_metric)
                try:
                    # read file
                    if(file_metric != 'None'):
                        
                        posEnd = file_metric.rfind('/')
                        file_name = file_metric[posEnd+1:]
                        server_ip = file_metric[6:posEnd]
                        smb = smbclient.SambaClient(server=server_ip, share="experimentos",
                                username='cloudmetric', password='nerds1203', domain='WORKGROUP')
                        json_file = smb.open(file_name, 'r+') 
                        
                        #json_file = open(file_metric, 'r+') 
                        #print(json_file) 
                        #first = json_file.read(0)
                        
                        #if(not first):
                        #    print('Arquivo vazio')
                        #    continue
                        message = json.load(json_file)
                        print(message)
                        #print(message)
                        #print('etapa 1')
                        #print(data)
                        #print(str(data['Timestamp']))
                            
                        #prinnt('Passou')    
                        #print(message)
                        #message = 'Transmitindo'
                        #Zera o tamanho do arquivo
                        json_file.truncate(0)
                        json_file.close()
                        broker.publicar('CloudMetric',topic,message)
                        broker.connection.close()
                    #print('Saindoooo')
                    #return
                except:
                    #print('entrou')
                    
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
