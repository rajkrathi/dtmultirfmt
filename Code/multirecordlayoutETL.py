import glob
import pypyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
import pypyodbc
import pandas as pd

#Process account sales record split
def fn_AcctSaleFile_Record(SalesFileLine,sept,fileName):
    SalesRecord=list()  
    # EntryType  11-19
    # SeqNbr 20-22
    # PostDate  23-34   
    # Account   35-47  
    # Description  48-75              
    # Source 76-78
    # Indicator 79-85
    # Reference 86-93    
    # Posted    94-98  
    # GrossSales   99-115
    # Returns 116-130
    EntryType=SalesFileLine[11:19].strip().replace("|","")
    SeqNbr=SalesFileLine[20:22].strip().replace("|","")
    PostDate=SalesFileLine[23:34].strip().replace("|","")
    Account=SalesFileLine[35:47].strip().replace("|","")
    Description=SalesFileLine[48:75].strip().replace("|","")
    Source=SalesFileLine[76:78].strip().replace("|","")
    Indicator=SalesFileLine[79:85].strip().replace("|","")
    Reference=SalesFileLine[86:93].strip().replace("|","")
    Posted=SalesFileLine[94:98].strip().replace("|","")
    GrossSales=SalesFileLine[99:115].strip().replace("|","")
    SalesReturns=SalesFileLine[116:130].strip().replace("|","")
    #print(SeqNbr,PostDate,Account,Description,Source,Indicator,Reference,PostDate,GrossSales,SalesReturns    )

    SalesRecord.append(fileName)
    SalesRecord.append(EntryType)
    SalesRecord.append(SeqNbr)
    SalesRecord.append(PostDate)
    SalesRecord.append(Account)
    SalesRecord.append(Description)
    SalesRecord.append(Source)
    SalesRecord.append(Indicator)
    SalesRecord.append(Reference)  
    SalesRecord.append(Posted)
    SalesRecord.append(GrossSales)   
    SalesRecord.append(SalesReturns)  
    #Merge dictionary into single record by split by seperator     
    SalesRecordFinal = sept.join(SalesRecord)
    SalesRecord.clear()
    return(SalesRecordFinal)
#process department sales record split
def fn_DeptSaleFile_Record(SalesFileLine,sept,fileName):
    SalesRecord=list()
    # EntryType 11-19
    # PostDate  19-29
    # Posted    30-34 
    # NetSales  35-75
    EntryType=SalesFileLine[11:19].strip().replace("|","") 
    PostDate=SalesFileLine[19:29].strip().replace("|","")
    Posted=SalesFileLine[30:34].strip().replace("|","")
    NetSales=SalesFileLine[35:75].strip().replace("|","") 
    #print(EntryType ,PostDate, Posted,NetSales)
 
    SalesRecord.append(fileName)
    SalesRecord.append(EntryType) 
    SalesRecord.append(PostDate)
    SalesRecord.append(Posted)
    SalesRecord.append(NetSales)  
    #Merge dictionary into single record by split by seperator     
    SalesRecordFinal = sept.join(SalesRecord)
    SalesRecord.clear()
    return(SalesRecordFinal)
#process item sales record split
def fn_ItemSaleFile_Record(SalesFileLine,sept,fileName):
    SalesRecord=list()
    # ItemID  - 11-49
    # UnitsAvailable - 50:60 
    ItemID=SalesFileLine[11:49].strip().replace("|","") 
    ItemNetUnits=SalesFileLine[50:60].strip().replace("|","") 
 
    SalesRecord.append(fileName)
    SalesRecord.append(ItemID) 
    SalesRecord.append(ItemNetUnits) 
    #print(ItemID,ItemNetUnits)
    #Merge dictionary into single record by split by seperator     
    SalesRecordFinal = sept.join(SalesRecord)
    SalesRecord.clear()
    return(SalesRecordFinal)
#process sales header record data split
def fn_HeaderSaleFile_Record(SalesFileLine,sept,fileName):
    SalesRecord=list()
    # FileDate 11-21 
    # Batch    22-30
    FileDate=SalesFileLine[11:21].strip().replace("|","") 
    Batch=SalesFileLine[22:30].strip().replace("|","") 
 
    SalesRecord.append(fileName)
    SalesRecord.append(FileDate) 
    SalesRecord.append(Batch) 
    #print(FileDate,Batch)
    #Merge dictionary into single record by split by seperator     
    SalesRecordFinal = sept.join(SalesRecord)
    SalesRecord.clear()
    return(SalesRecordFinal)
#process sales summary record data split
def fn_SummarySaleFile_Record(SalesFileLine,sept,fileName):
    SalesRecord=list()
    # FileDate 11-21 
    # Batch    22-30
    RecordFileName=SalesFileLine[11:23].strip().replace("|","") 
    RecordCount=SalesFileLine[24:30].strip().replace("|","") 
 
    SalesRecord.append(fileName)
    SalesRecord.append(RecordFileName)
    SalesRecord.append(RecordCount) 
    #print(FileName,RecordCount)
    #Merge dictionary into single record by split by seperator     
    SalesRecordFinal = sept.join(SalesRecord)
    SalesRecord.clear()
    return(SalesRecordFinal)
#Create header record for sales summary
def fn_SummarySaleFile_Header (sept):
    SalesLine=list() 
    ### Create header line item file header record 
    SalesLine.append("FileName")
    SalesLine.append("RecordName")
    SalesLine.append("RecordCount") 
    SalesLineRecord = sept.join(SalesLine)  
    SalesLine.clear()
    return(SalesLineRecord)
#create header record for sales header
def fn_HeaderSaleFile_Header (sept):
    SalesLine=list() 
    ### Create header line item file header record 
    SalesLine.append("FileName")
    SalesLine.append("FileDate")
    SalesLine.append("Batch") 
    SalesLineRecord = sept.join(SalesLine)  
    SalesLine.clear()
    return(SalesLineRecord)
#create header record for item sales file
def fn_ItemSaleFile_Header (sept):
    SalesLine=list() 
    ### Create header line item file header record 
    SalesLine.append("FileName")
    SalesLine.append("ItemID")
    SalesLine.append("NetAvailableUnitsForSale") 
    SalesLineRecord = sept.join(SalesLine)  
    SalesLine.clear()
    return(SalesLineRecord)
#create header record for department sales file
def fn_DeptSaleFile_Header (sept):
    SalesLine=list() 
    ### Create header line item file header record 
    SalesLine.append("FileName")
    SalesLine.append("EntryType")
    SalesLine.append("PostDate") 
    SalesLine.append("Posted")  
    SalesLine.append("NetSales") 
    SalesLineRecord = sept.join(SalesLine)  
    SalesLine.clear()
    return(SalesLineRecord)
#create header record for account sales file
def fn_AccountSaleFile_Header (sept):
    SalesLine=list() 
    ### Create header line item file header record 
    SalesLine.append("FileName")
    SalesLine.append("EntryType")
    SalesLine.append("SeqNbr")
    SalesLine.append("PostDate") 
    SalesLine.append("AccountNumber")  
    SalesLine.append("Description")  
    SalesLine.append("SourceDescription") 
    SalesLine.append("Indicator") 
    SalesLine.append("Reference") 
    SalesLine.append("Posted")  
    SalesLine.append("GrossSales")  
    SalesLine.append("SalesReturns") 
    SalesLineRecord = sept.join(SalesLine)  
    SalesLine.clear()
    return(SalesLineRecord)
AcctSaleFile = open("C:\FixedWidthMultipleLayout\OutputFiles\\SaleAcct.txt", "w")
DeptSaleFile = open("C:\FixedWidthMultipleLayout\OutputFiles\\SaleDepartment.txt", "w")
ItemSaleFile = open("C:\FixedWidthMultipleLayout\OutputFiles\\SaleItem.txt", "w")
SummarySaleFile = open("C:\FixedWidthMultipleLayout\OutputFiles\\SaleSummary.txt", "w")
HeaderSaleFile = open("C:\FixedWidthMultipleLayout\OutputFiles\\SaleHeader.txt", "w")
sept = "|"
### Create Header records in output file by sep.
AcctSaleFile.write(fn_AccountSaleFile_Header(sept)+ '\n')
DeptSaleFile.write(fn_DeptSaleFile_Header(sept)+ '\n')
ItemSaleFile.write(fn_ItemSaleFile_Header(sept)+ '\n')
SummarySaleFile.write(fn_SummarySaleFile_Header(sept)+ '\n')
HeaderSaleFile.write(fn_HeaderSaleFile_Header(sept)+ '\n')

#For each record find type of records. Process each type of record
for fileName in glob.glob('C:\FixedWidthMultipleLayout\InputFiles\s*.txt'):
    #print(fileName)
    Salesfile = open(fileName) 
    #### Read the file and process data
    SalesFileLine=Salesfile.readline()
    while SalesFileLine!='':
        # code to pad spaces in string 
        SalesFileLine = "{:<120}".format(SalesFileLine).replace("|","")  
        RecordTypeCode=SalesFileLine[0:11].strip() 
        #print(RecordTypeCode)
        if RecordTypeCode == 'AcctSale':                  
            AcctSaleFileRecord=fn_AcctSaleFile_Record(SalesFileLine,sept,fileName)
            AcctSaleFile.write(AcctSaleFileRecord+ '\n')       
            #print(1)
        if RecordTypeCode == 'Department':  
            DeptSaleFileRecord=fn_DeptSaleFile_Record(SalesFileLine,sept,fileName)
            DeptSaleFile.write(DeptSaleFileRecord+ '\n')      
        if RecordTypeCode == 'Item': 
            ItemSaleFileRecord=fn_ItemSaleFile_Record(SalesFileLine,sept,fileName)
            ItemSaleFile.write(ItemSaleFileRecord+ '\n')  
        if RecordTypeCode == 'Summary':  
            SummarySaleFileRecord=fn_SummarySaleFile_Record(SalesFileLine,sept,fileName)
            SummarySaleFile.write(SummarySaleFileRecord+ '\n')   
        if RecordTypeCode == 'Header': 
            HeaderSaleFileRecord=fn_HeaderSaleFile_Record(SalesFileLine,sept,fileName)
            HeaderSaleFile.write(HeaderSaleFileRecord+ '\n')   
        SalesFileLine=Salesfile.readline()
Salesfile.close()
AcctSaleFile.close()
DeptSaleFile.close()
ItemSaleFile.close()
SummarySaleFile.close()
HeaderSaleFile.close()

#Read the above created files panda  DF to load sql server
dfSaleAcct = pd.read_csv('C:\FixedWidthMultipleLayout\OutputFiles\\SaleAcct.txt', sep="|")
dfSaleDepartment = pd.read_csv('C:\FixedWidthMultipleLayout\OutputFiles\\SaleDepartment.txt', sep="|") 
dfSaleItem = pd.read_csv('C:\FixedWidthMultipleLayout\OutputFiles\\SaleItem.txt', sep="|")
dfSaleSummary = pd.read_csv('C:\FixedWidthMultipleLayout\OutputFiles\\SaleSummary.txt', sep="|")
dfSaleHeader = pd.read_csv('C:\FixedWidthMultipleLayout\OutputFiles\\SaleHeader.txt', sep="|")
## Add the current date to dataframe to write to SQL table
dfSaleAcct['CreateDate'] = pd.to_datetime('now')
dfSaleDepartment['CreateDate'] = pd.to_datetime('now')
dfSaleItem['CreateDate'] = pd.to_datetime('now')
dfSaleSummary['CreateDate'] = pd.to_datetime('now')
dfSaleHeader['CreateDate'] = pd.to_datetime('now')

## Use DF to SQL write function to load the files creaeted by above process
import sqlalchemy as sal
from sqlalchemy import create_engine
sqlDriver='driver=ODBC+Driver+17+for+SQL+Server' 
server = 'tcp:server.abc.com'
database = 'dbname'
username = 'username'
password = 'password'
sqlAlchemyConnection='mssql+pyodbc://'+username+':'+password+'@' +server +'/'+database +'?'+sqlDriver
engine = create_engine(sqlAlchemyConnection)
dfSaleAcct.to_sql('SaleAcct', engine, if_exists='replace', index=False, schema='stage')
dfSaleDepartment.to_sql('SaleDepartment', engine, if_exists='replace', index=False, schema='stage')
dfSaleItem.to_sql('SaleItem', engine, if_exists='replace', index=False, schema='stage')
dfSaleSummary.to_sql('SaleSummary', engine, if_exists='replace', index=False, schema='stage')
dfSaleHeader.to_sql('SaleHeader', engine, if_exists='replace', index=False, schema='stage')

#Process the above loaded SQL tables
connString='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD=' + password 
cnxn = pypyodbc.connect(connString)
cursor = cnxn.cursor()
cursor.execute("[dbo].[usp_Process]") 
cursor.close()
cnxn.commit()
cnxn.close()
