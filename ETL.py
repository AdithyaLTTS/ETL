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

#tbl_registration

root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_registration*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_registration']
        Registration = pd.DataFrame(Reg_Dict, columns = ['col_id','col_registrationno','col_titleid','col_displayname','col_dateofbirth','col_religionid','col_gender','col_nationalityid','col_cityareaid','col_mothername','col_fathername','col_email','col_mobileno','col_localaddress','col_localaddress2','col_localcity'])
    with open('Registration.json', 'w') as output_file:
        json.dump(Reg_Dict, output_file)
    print(Reg_Dict)
#tbl_admission

root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_admission*.json', recursive=True):
    #print("File Name: ", path.split('\\')[-1])
    with open(path, 'r') as j:
        contents = json.loads(j.read())
    Reg_Dict = contents['tbl_admission']
    Admission = pd.DataFrame(Reg_Dict, columns = ['col_admissiondate','col_admittingdoctorid','col_dependentname', 'col_registrationid', 'col_encounterid'])

#tbl_citymaster

root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_citymaster*.json', recursive=True):
    #print("File Name: ", path.split('\\')[-1])
    with open(path, 'r') as j:
        contents = json.loads(j.read())
    Reg_Dict = contents['tbl_citymaster']
    CityMaster = pd.DataFrame(Reg_Dict, columns = ['col_cityid', 'col_cityname'])

#tbl_employee

root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_employee*.json', recursive=True):
    #print("File Name: ", path.split('\\')[-1])
    with open(path, 'r') as j:
        contents = json.loads(j.read())
    Reg_Dict = contents['tbl_employee']
    Employee = pd.DataFrame(Reg_Dict,  columns = ['col_firstname', 'col_id', 'col_departmentid'])
    
#tbl_encounter

root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_encounter*.json', recursive=True):
    #print("File Name: ", path.split('\\')[-1])
    with open(path, 'r') as j:
        contents = json.loads(j.read())
    Reg_Dict = contents['tbl_encounter']
    Encounter = pd.DataFrame(Reg_Dict, columns = ['col_id','col_opip','col_encounterno','col_encounterdate','col_doctorid', 'col_registrationid'])

#tbl_departmentmain

root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_DepartmentMain*.json', recursive=True):
    #print("File Name: ", path.split('\\')[-1])
    with open(path, 'r') as j:
        contents = json.loads(j.read())
    Reg_Dict = contents['tbl_DepartmentMain']
    DepartmentMain = pd.DataFrame(Reg_Dict,  columns = ['col_departmentid'])
    
#Renaming Columns with same Names
DepartmentMain.rename(columns = {'col_departmentid':'departmentmain_col_departmentid'}, inplace = True)
Employee.rename(columns = {'col_id':'employee_col_id'}, inplace = True)
Encounter.rename(columns = {'col_id':'encounter_col_id'}, inplace = True)
Encounter.rename(columns = {'col_registrationid':'encounter_col_registrationid'}, inplace = True)

#Algorithm To Extract Data for Use-Case 01 - PatientInfo
#Registration -> CityMaster
Merge1 = pd.merge(Registration, CityMaster, how='left', left_on=[
                     'col_localcity'], right_on=['col_cityid']).fillna(value={'col_cityname':''})
#Encounter -> Employee
Merge2 = pd.merge(Encounter, Employee, how='left', left_on=[
                     'col_doctorid'], right_on=['employee_col_id']).fillna(value={'col_firstname':''})                    
#Encounter -> Employee -> DepartmentMain
Merge3 = pd.merge(Merge2, DepartmentMain, how='left', left_on=[
                     'col_departmentid'], right_on=['departmentmain_col_departmentid']).fillna(value={'col_departmentname':''})
#Registration -> CityMaster -> Encounter -> Employee - > DepartmentMain
Merge4 = pd.merge(Merge1, Merge3, how='inner', left_on=[
                     'col_id'], right_on=['encounter_col_registrationid'])
#Registration -> CityMaster -> Encounter -> Employee - > DepartmentMain -> Admission
Merge5 = pd.merge(Merge4, Admission, how='inner', left_on=[
                     'col_id', 'encounter_col_id'], right_on=['col_registrationid', 'col_encounterid'])

#Droping Unwanted Columns From DataFrame
Merge5.drop(['col_localcity', 'encounter_col_id', 'encounter_col_id', 'encounter_col_registrationid', 'col_firstname', 'employee_col_id', 'col_departmentid', 'departmentmain_col_departmentid', 'col_registrationid', 'col_encounterid'], inplace=True, axis=1)

#Connecting To MySQL Server and Create Table Inside Database & Pushing DataFrame To MySQL Database
# Create SQLAlchemy engine to connect to MySQL Database
hostname="localhost"
dbname="etl-test"
uname="root"
pwd="Toor"
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))

# Convert dataframe to sql table                                   
Merge5.to_sql('patient', engine, index=False)