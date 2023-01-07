import os
import re
import pandas as pd
from datetime import datetime
import shutil
import sqlalchemy as sa


def loadTextFiles(fileName, path):
    filePath = '/'.join([path, fileName])
    df = pd.read_csv(filepath_or_buffer=filePath, delimiter=' | ', header=None).dropna(axis = 1, how = "all")
    return df
    
        
def makeDataClean(data, fileType):
    for col in range(data.shape[1]):
        data.iloc[:, col] = data.iloc[:, col].str.replace('^.*?:','', regex = True).str.strip()
        
    if fileType == "block":
        data.AccountNumber = data.AccountNumber.str.replace('\\*', '', regex = True).str.strip()
        data.ShebaNumber = data.ShebaNumber.str.replace('\\*', '', regex = True).str.strip()
    
    return data
        
def enrichData(data, file, fileType):
    data['FileName'] = file
    data['Type'] = fileType
    
    return data
    
def createPickle(data, fileType):
    if fileType == "block":
        data.to_pickle('Pickles/block.pickle')
    elif fileType == "blockErrors":
        data.to_pickle('Pickles/blockErrors.pickle')
    elif fileType == "transferBlock":
        data.to_pickle('Pickles/transferBlock.pickle')
    elif fileType == "transferBlockErrors":
        data.to_pickle('Pickles/transferBlockErrors.pickle')
    elif fileType == "unblock":
        data.to_pickle('Pickles/unblock.pickle')
    elif fileType == "unblockErrors":
        data.to_pickle('Pickles/unblockErrors.pickle')
    

def moveLogs(path, files):
    folderName = datetime.strftime(datetime.now(), '%Y-%m-%d %H%M')
    os.makedirs('/'.join([path, folderName]))
    for file in files:
        shutil.move('/'.join([path, file]), '/'.join([path, folderName, file]))
        
def enrichUnresolved(unresolved):
    unresolved.drop(['FileName', 'Type', 'EndedUpIn'], axis = 1, inplace = True)
    unresolved.BankName = 'بانک:' + unresolved.BankName
    unresolved.AccountNumber = 'حساب:' + unresolved.AccountNumber
    unresolved.ShebaNumber = 'شبا:' + unresolved.ShebaNumber
    unresolved.Amount = 'مبلغ:' + unresolved.Amount
    unresolved.BlockCode = 'کد مسدودی:' + unresolved.BlockCode
    unresolved.Date = 'تاریخ:' + unresolved.Date
    unresolved.TransactionTime = 'زمان تراکنش:' + unresolved.TransactionTime
    unresolved.ReferenceCode = 'کد پیگیری:' + unresolved.ReferenceCode
    unresolved.Status = 'وضعیت:' + unresolved.Status
    
    return unresolved

def writeUnresolvedBlocks(unresolved, path):
    unresolved.to_csv('/'.join([path, 'block2.txt']), index = None, header=None, sep = '\t', mode='a')
    
def createEngine():
    config = 'mssql+pyodbc://172.16.1.119/SadeghiTest?driver=SQL+Server+Native+Client+11.0'
    return sa.create_engine(config)

def setDBTypes(fileType):
    if fileType == "block":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=255), 
                 "FileName": sa.types.NVARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30), 
                 "EndedUpIn": sa.types.VARCHAR(length=13)}
    elif fileType == "blockErrors":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "ErrorCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=1000), 
                 "FileName": sa.types.NVARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "transferBlock":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "SourceAccount": sa.types.VARCHAR(length=50), 
                 "DestinationAccount": sa.types.VARCHAR(length=50),
                 "BlockAmount": sa.types.VARCHAR(length=50), 
                 "DocumentNumber": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "TransferAmount": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=255), 
                 "Debit": sa.types.VARCHAR(length=50), 
                 "Credit": sa.types.VARCHAR(length=50), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "transferBlockErrors":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "SourceAccount": sa.types.VARCHAR(length=50), 
                 "DestinationAccount": sa.types.VARCHAR(length=50),
                 "BlockAmount": sa.types.VARCHAR(length=50), 
                 "DocumentNumber": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "TransferAmount": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "ErrorCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "unblock":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=255), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=30)}
    elif fileType == "unblockErrors":
        dtype = {"BankName":  sa.types.NVARCHAR(length=50), 
                 "AccountNumber": sa.types.VARCHAR(length=50), 
                 "ShebaNumber": sa.types.VARCHAR(length=50),
                 "Amount": sa.types.VARCHAR(length=50), 
                 "BlockCode": sa.types.VARCHAR(length=50), 
                 "Date": sa.types.VARCHAR(length=50), 
                 "TransactionTime": sa.types.VARCHAR(length=21), 
                 "ReferenceCode": sa.types.VARCHAR(length=50), 
                 "ErrorCode": sa.types.VARCHAR(length=50), 
                 "Status": sa.types.NVARCHAR(length=1000), 
                 "FileName": sa.types.VARCHAR(length=255), 
                 "Type": sa.types.VARCHAR(length=15)}
    return dtype

def fixColumnSize(data):
    data.BankName = data.BankName.str[:50]
    return 
