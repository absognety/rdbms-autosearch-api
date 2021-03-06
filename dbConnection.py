### db dictionary prep starts here ##
    
import mysql.connector
import pandas as pd
# DB connection
class DBConnect(object):
    def __init__(self,hostname,username,
                 password):
        self.hostname = hostname
        self.username = username
        self.password = password
        
    def makeConnection(self):
        
        slave_conn = mysql.connector.connect(
          host=self.hostname,
          user=self.username,
          passwd=self.password
        )
        if slave_conn.is_connected():
            print ("Connection Successful to {}".format(slave_conn.server_host))
        else:
            print ("Connection failed")
        self.conn = slave_conn
        return slave_conn

    def run_query(self,conn,query,use=False):
        conn_cursor = conn.cursor()
        if use == False:
            conn_cursor.execute(query)
            tuple_list = conn_cursor.fetchall()
            return tuple_list
        else:
            conn_cursor.execute(query)
            return 0
    
    def prepare_data(self,conn,db,tbs,colslist,n):
        colslist = ['Database','Table'] + colslist
        df_cols = pd.DataFrame(index=range(n),
                               columns = colslist)
        df_cols['Database'] = [db[0]] * n
        ind=0
        for tb in tbs:
            if '.' in tb[0] and tb[0].split('.')[0] == db[0]:
                tb = (tb[0].split('.')[1],)
            colsquery = "show columns from " + db[0] + '.' + tb[0] + ';'
            cols = self.run_query(conn,colsquery)
            colnames = list(map(lambda x: x[0],cols))
            df_cols.loc[ind,'Table'] = tb[0]
            df_cols.loc[ind,colnames] = 1
            ind += 1
        return df_cols
    
    def search_table(self,conn,dbs,tb_name):
        result = []
        for db in dbs:
            if ((db[0] == 'information_schema') |
                (db[0] == 'performance_schema') |
                (db[0] == 'sys') |
                (db[0] == 'mysql')):
                continue
            else:
                use_query = "use " + db[0] + ";"
                use_db = self.run_query(conn,use_query,use=True)
                tables = self.run_query(conn,"show tables;")
                tables = [i[0] for i in tables]
                if tb_name in tables:
                    result.append({'Database':db[0],'Table':tb_name})
                else:
                    continue
        if not result:
            print ("the table with name {} does not exist".format(tb_name))
        else:
            return result
