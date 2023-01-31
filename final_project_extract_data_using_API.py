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


# chicago_dataset = pd.read_csv('jcxq-k9xf.csv')
## create table
# createQuery = create_table_query('chicago_socioeconomic_data3', chicago_dataset)
# ibm_db.exec_immediate(conn, createQuery)

# try:
# 	## Insert data into table. if table exists already, fail to insert. Change options for overwrite and append
# 	print(chicago_dataset.to_sql('chicago_socioeconomic_data', engine, if_exists='replace', index_label='ID'))
# except ValueError as ve:
# 	print("Failed: ", ve)

# import matplotlib.pyplot as plt
# import seaborn as sns

# income_vs_hardship = pd.read_sql("SELECT per_capita_income_, hardship_index FROM chicago_socioeconomic_data;", pconn)

# plot = sns.jointplot(x='PER_CAPITA_INCOME_',y='HARDSHIP_INDEX', data=income_vs_hardship)
# plt.show()
# Find the total number of crimes recorded in the CRIME table.
p1 = "Select count(*) from CHICAGO_CRIME_DATA"
# List community areas with per capita income less than 11000.
p2 = "select * from census_data\
    where per_capita_income < 11000"
# List all case numbers for crimes involving minors?(children are not considered minors for the purposes of crime analysis)
p3 = "select case_number from chicago_crime_data\
    where description like '%MINOR%'"
# List all kidnapping crimes involving a child?
p4 = "select primary_type from chicago_crime_data\
    where primary_type = 'KIDNAPPING'\
        and description like '%CHILD%'"
# What kinds of crimes were recorded at schools?
p5 = "select primary_type from chicago_crime_data\
    where location_description like '%SCHOOL%'"
# List the average safety score for each type of school.
p6 = "select distinct \"Elementary, Middle, or High School\", avg(safety_score) from SCHOOLS\
    group by \"Elementary, Middle, or High School\""
# List 5 community areas with highest % of households below poverty line
p7 = "select * from chicago_socioeconomic_data\
	order by PERCENT_HOUSEHOLDS_BELOW_POVERTY desc limit 5"
# Which community area is most crime prone?
p8 = "select count(COMMUNITY_AREA_NUMBER) as ca from CHICAGO_CRIME_DATA\
	group by COMMUNITY_AREA_NUMBER\
    	order by ca desc\
            limit 1"
# Use a sub-query to find the name of the community area with highest hardship index
p9 = "select community_area_name from census_data\
	where hardship_index = (select max(hardship_index) from census_data)"
# Use a sub-query to determine the Community Area Name with most number of crimes?
p10 = "select community_area_name from census_data\
	where community_area_number = (select COMMUNITY_AREA_NUMBER from CHICAGO_CRIME_DATA\
        group by COMMUNITY_AREA_NUMBER\
	        order by count(COMMUNITY_AREA_NUMBER) desc\
	            limit 1)"
pd.set_option('display.max_rows', None)
print(pd.read_sql(p10, pconn))