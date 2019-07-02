import mysql.connector
from mysql.connector import Error

class MySQLCloudMetric():
    def __init__(self):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
    def select_metric_by_active(self,db_file):
        """
        Query tasks 
        :param conn: the Connection object
        :param priority:
        :return:
        """
        try:
            self.con = mysql.connector.connect(host='localhost',
                             database='cloudmetric',
                             user='cloudmetric',
                             password='cloudmetric')
        except:
            self.conn = None
            self.conn.close()
            exit(1)
        if (self.con.is_connected()):
            #db_Info = self.con.get_server_info()
            #print("Connected to MySQL database... MySQL Server version on ",db_Info)
            self.cur = self.con.cursor()
            self.cur.execute("""select m.id, m.pathIn, m.topic, b.* from metric_metric as m inner 
            join broker_broker as b where m.ativo = 1 and m.metric_broker_id = b.id""")
            rows = self.cur.fetchall()
            self.con.close()
            return rows


    def update_metric(self,db_file):
        try:
            self.con = mysql.connector.connect(host='localhost',
                database='cloudmetric',
                user='cloudmetric',
                password='cloudmetric')
            self.cur = self.con.cursor()
            print('Status atualizado com sucesso')
            self.cur.execute("update metric_metric  set ativo = 0 where id = '8f350fcaf0424420b9864c58da593ba7'")
            self.con.commit()
            self.con.close()
        except:
            self.con = None
            print('Erro ao atualizar o status')
            self.con.close()
            exit(1)

