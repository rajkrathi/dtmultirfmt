import pypyodbc 
import pandas as pd

#SQL server data from Amazon
server = 'tcp:sqlsrv01.XXX.rds.amazonaws.com' 
database = 'DB1' 
username = 'U1' 
password = 'P1' 
cnxn = pypyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
cursor.execute("SELECT   * FROM dbo.Table1") 
# Put it all to a data frame
pdDF = pd.DataFrame(cursor.fetchall())
pdDF.columns = [column[0] for column in cursor.description] 
cursor.close()
cnxn.close()

#Move to GCP mysql
import pymysql
from sqlalchemy import create_engine  
engine = create_engine("mysql+pymysql://UID:PWD@IP/DBName")
con=engine.connect() 
GP_Transactions.to_sql(name='Table1', con=con, if_exists = 'replace', index=False) 
con.close()
