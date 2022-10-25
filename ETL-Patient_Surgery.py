#importing Header Files
import pandas as pd
import json
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import logging
import os, json
import glob
import pathlib

#Extracting Data out of Json Files
#tbl_admission

Admission = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_admission*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_admission']
        Admission1 = pd.DataFrame(Reg_Dict, columns = ['col_admittingdoctorid', 'col_encounterid'])
        Admission = pd.concat([Admission, Admission1])

#tbl_employee

Employee = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_employee*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_employee']
        Employee1 = pd.DataFrame(Reg_Dict, columns = ['col_departmentid', 'col_id'])
        Employee = pd.concat([Employee, Employee1])

#tbl_departmentmain

DepartmentMain = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_departmentmain*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_DepartmentMain']
        DepartmentMain1 = pd.DataFrame(Reg_Dict, columns = ['col_departmentname', 'col_departmentid'])
        DepartmentMain = pd.concat([DepartmentMain, DepartmentMain1])

#tbl_surgeryorder

SurgeryOrder = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_surgeryorder*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_surgeryorder']
        SurgeryOrder1 = pd.DataFrame(Reg_Dict, columns = ['col_encounterid', 'col_registrationid', 'col_otsdate', 'col_otedate', 'col_otroomid'])
        SurgeryOrder = pd.concat([SurgeryOrder, SurgeryOrder1])
print(SurgeryOrder)
#Renaming Columns with same Names
DepartmentMain.rename(columns = {'col_departmentid':'departmentmain_col_departmentid'}, inplace = True)
Employee.rename(columns = {'col_id':'employee_col_id'}, inplace = True)
Admission.rename(columns = {'col_encounterid':'admission_col_encounterid'}, inplace = True)

#Algorithm To Extract Data for Use-Case 01 - PatientInfo
#SurgeryOrder -> Admission
Merge1 = pd.merge(SurgeryOrder, Admission, how='inner', left_on=[
                     'col_encounterid'], right_on=['admission_col_encounterid'])
#SurgeryOrder -> Admission -> Employee
Merge2 = pd.merge(Merge1, Employee, how='inner', left_on=[
                     'col_admittingdoctorid'], right_on=['employee_col_id'])                      
#SurgeryOrder -> Admission -> Employee -> DepartmentMain
Merge3 = pd.merge(Merge2, DepartmentMain, how='inner', left_on=[
                     'col_departmentid'], right_on=['departmentmain_col_departmentid'])
#Droping Unwanted Columns From DataFrame
Merge3.drop(['admission_col_encounterid', 'employee_col_id', 'departmentmain_col_departmentid'], inplace=True, axis=1)

# dropping ALL duplicate values
Merge3.drop_duplicates(keep='first', inplace = True)

#Connecting To MySQL Server and Create Table Inside Database & Pushing DataFrame To MySQL Database
# Create SQLAlchemy engine to connect to MySQL Database
hostname="localhost"
dbname="etl-test"
uname="root"
pwd="Toor"
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))

# Convert dataframe to sql table                                   
Merge3.to_sql('patient', engine, index=False)