import ibm_db
import ibm_db_dbi
import pandas as pd
from sqlalchemy import create_engine


def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar(16384)')
    return dataList

def create_table_query(table_name: str, df: pd.DataFrame) -> str:
	columnsDatatype = getColumnDtypes(df.dtypes)

	columns = ''
	for (ind,n) in enumerate(df.dtypes.index, start=0):
		columns += '\t' + n + ' ' + columnsDatatype[ind] + ',\n'
	columns = columns[:-2]

	return f'''create table if not exists {table_name}(
		{columns})'''

dsn_hostname = "ba99a9e6-d59e-4883-8fc0-d6a8c9f7a08f.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud" # e.g.: "54a2f15b-5c0f-46df-8954-7e38e612c2bd.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"
dsn_uid = "fxm42129"        # e.g. "abc12345"
dsn_pwd = "uLTqraD92LIJ14xL"      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BLUDB"            # e.g. "BLUDB"
dsn_port = "31321"                # e.g. "32733" 
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

# sql_url = "db2://fxm42129:uLTqraD92LIJ14xL@ba99a9e6-d59e-4883-8fc0-d6a8c9f7a08f.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud:31321/bludb;Security=ssl;"
# try:
# 	engine = create_engine(sql_url)
# except:
# 	print('failed to connect')

try:
	conn = ibm_db.connect(dsn, "", "")
	pconn = ibm_db_dbi.Connection(conn)
	print("Connection to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
except:
	print("Unable to connect: ", ibm_db.conn_errormsg())


chicago_dataset = pd.read_csv('jcxq-k9xf.csv')
print(chicago_dataset.dtypes.index)
## create table
# createQuery = create_table_query('chicago_socioeconomic_data3', chicago_dataset)
# ibm_db.exec_immediate(conn, createQuery)

# try:
# 	## Insert data into table. if table exists already, fail to insert. Change options for overwrite and append
# 	print(chicago_dataset.to_sql('chicago_socioeconomic_data', engine, if_exists='replace', index_label='ID'))
# except ValueError as ve:
# 	print("Failed: ", ve)

import matplotlib.pyplot as plt
import seaborn as sns

income_vs_hardship = pd.read_sql("SELECT per_capita_income_, hardship_index FROM chicago_socioeconomic_data;", pconn)

plot = sns.jointplot(x='PER_CAPITA_INCOME_',y='HARDSHIP_INDEX', data=income_vs_hardship)
plt.show()