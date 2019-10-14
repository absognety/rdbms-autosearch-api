import subprocess
import sys
import dbConnection
import pandas as pd

reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])
installed_packages = [r.decode().split('==')[0] for r in reqs.split()]

package_name = 'mysql-connector'
if package_name not in installed_packages:
    subprocess.check_call(['python','-m','pip','install',package_name])
    
#establish slave-db mysql connection 

hostname = "slave.camv7qolninq.ap-south-1.rds.amazonaws.com"
username = "tech"
password = "[%2!jQpem&-$;Sk#7KdG"
db_conn = dbConnection.DBConnect(hostname = hostname,
                                 username = username,
                                 password = password)
slave_conn = db_conn.makeConnection()
dbs = db_conn.run_query(slave_conn,"show databases;")

total_df = pd.DataFrame()
#dbs = [('abtest',),('abtest_baseline',),('analytics',)]
for db in dbs:
    if ((db[0] == 'information_schema') |
        (db[0] == 'performance_schema') |
        (db[0] == 'sys') |
        (db[0] == 'mysql')):
        continue
    else:
        use_query = "use " + db[0] + ";"
        use_db = db_conn.run_query(slave_conn,use_query,use=True)
        tables = db_conn.run_query(slave_conn,"show tables;")
        ntables = len(tables)
        print (ntables)
        uniq_cols = []
        for tb in tables:
            if '.' in tb[0] and tb[0].split('.')[0] == db[0]:
                tb = (tb[0].split('.')[1],)
            colsquery = "show columns from " + db[0] + '.' + tb[0] + ';'
            print (tb[0])
            cols = db_conn.run_query(slave_conn,colsquery)
            colnames = list(map(lambda x: x[0],cols))
            uniq_cols.extend(colnames)
        uniq_cols = list(set(uniq_cols))
        flags_df = db_conn.prepare_data(slave_conn,db,tables,uniq_cols,ntables)
        print (flags_df.shape)
        total_df = total_df.append(flags_df)

total_df.fillna(0,inplace=True)
total_df.to_csv('Complete-SlaveDB-dictionary.csv',index=False)