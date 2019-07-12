import mysql.connector
from mysql.connector import Error

class MySQLCloudMetric():
    def __init__(self):
        try:
            self.con = mysql.connector.connect(host='localhost',
                                database='cloudmetric',
                                user='cloudmetric',
                                password='cloudmetric')
            print('Banco de Dados Conectado com sucesso')
        except mysql.connector.Error as err:
            print("Banco de dados nao conectado: {}".format(err))
            self.conn = None
            #self.conn.close()
            exit(1)
        
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
    def select_metric_by_active(self):
        """
        Query tasks 
        :param conn: the Connection object
        :param priority:
        :return:
        """
        #print(self.con.is_connected())
        if (self.con.is_connected()):
            #db_Info = self.con.get_server_info()
            #print("Connected to MySQL database... MySQL Server version on ",db_Info)
            #print('Selecionando metricas por atividade')
            self.cur = self.con.cursor(buffered=True)
            self.cur.execute("""select m.id, m.pathIn, m.topic, b.* from metric_metric as m inner 
            join broker_broker as b where m.ativo = 1 and m.metric_broker_id = b.id""")
            self.con.commit()
            rows = self.cur.fetchall()
            #print(rows)
            self.cur.close()
            return rows


    def update_metric(self,id_metric):
        try:
            print('Atualizando o status da Metrica')
            #self.con = mysql.connector.connect(host='localhost',
            #    database='cloudmetric',
            #    user='cloudmetric',
            #    password='cloudmetric')
            self.cur = self.con.cursor() 
            update_metric_disable = ("update metric_metric  set ativo = 0 where id =(%s)")
            data = (id_metric)
            self.cur.execute(update_metric_disable,(data,))
            #print(str(id_metric))
            self.con.commit()
            self.cur.close()
            print('Status atualizado com sucesso')
        except mysql.connector.Error as err:
            print("Erro ao atualizar o Status: {}".format(err))
            

