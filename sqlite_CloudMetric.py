import sqlite3
from sqlite3 import Error

class SqLiteCloudMetric():
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
            self.conn = sqlite3.connect(db_file)
        except:
            self.conn = None
            self.conn.close()
            exit(1)
        self.cur = self.conn.cursor()
        self.cur.execute("""select m.id, m.pathIn, m.topic, b.* from metric_metric as m inner 
        join broker_broker as b where m.ativo = 1 and m.metric_broker_id = b.id""")
        rows = self.cur.fetchall()
        self.conn.close()
        return rows

    def update_metric(self,db_file):
        try:
            self.conn = sqlite3.connect(db_file)
        except:
            self.conn = None
            self.conn.close()
            exit(1)
        try:
            self.cur = self.conn.cursor()
            print('Status atualizado com sucesso')
            self.cur.execute("update metric_metric  set ativo = 0 where id = '99f07011d63f4da3a50206fa73e44417'")
            self.conn.commit()
            self.conn.close()
        except:
            print('Erro ao atualizar o status')
            self.conn.close()
            