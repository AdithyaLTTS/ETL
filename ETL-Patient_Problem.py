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
#tbl_registration

Registration = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_registration*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_registration']
        Registration1 = pd.DataFrame(Reg_Dict, columns = ['col_id','col_registrationno','col_titleid','col_displayname','col_dateofbirth','col_religionid','col_nationalityid','col_cityareaid','col_mothername','col_fathername'])
        Registration = pd.concat([Registration, Registration1])
        
#tbl_encounter

Encounter = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_encounter*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_encounter']
        Encounter1 = pd.DataFrame(Reg_Dict, columns = ['col_id','col_opip','col_encounterno','col_encounterdate','col_doctorid', 'col_registrationid'])
        Encounter = pd.concat([Encounter, Encounter1])

#tbl_emrpatientproblemdetails

EMRPPD = pd.DataFrame()
root_dir = r"D:\OneDrive - LTTS\Desktop\ETL-SynData\Json"
for path in glob.glob(f'{root_dir}/**/tbl_emrpatientproblemdetails*.json', recursive=True):
    with open(path, 'r') as infile:
        contents = json.loads(infile.read())
        Reg_Dict = contents['tbl_emrpatientproblemdetails']
        EMRPPD1 = pd.DataFrame(Reg_Dict, columns = ['col_problemdescription', 'col_registrationid', 'col_encounterid'])
        EMRPPD = pd.concat([EMRPPD, EMRPPD1])

#Renaming Columns with same Names
EMRPPD.rename(columns = {'col_registrationid':'EMRPPD_col_registrationid'}, inplace = True)
EMRPPD.rename(columns = {'col_encounterid':'EMRPPD_col_encounterid'}, inplace = True)
Encounter.rename(columns = {'col_id':'encounter_col_id'}, inplace = True)
Encounter.rename(columns = {'col_registrationid':'encounter_col_registrationid'}, inplace = True)

#Algorithm To Extract Data for Use-Case 01 - PatientInfo
#Registration -> Encounter
Merge1 = pd.merge(Registration, Encounter, how='inner', left_on=[
                     'col_id'], right_on=['encounter_col_registrationid'])
#Registration -> Encounter -> EMRPatientProb
Merge2 = pd.merge(Merge1, EMRPPD, how='inner', left_on=[
                     'col_id', 'encounter_col_id'], right_on=['EMRPPD_col_registrationid', 'EMRPPD_col_encounterid'])

#Droping Unwanted Columns From DataFrame
Merge2.drop(['encounter_col_id', 'encounter_col_registrationid', 'EMRPPD_col_registrationid', 'EMRPPD_col_encounterid'], inplace=True, axis=1)

# dropping ALL duplicate values
Merge2.drop_duplicates(keep='first', inplace = True)

#Connecting To MySQL Server and Create Table Inside Database & Pushing DataFrame To MySQL Database
# Create SQLAlchemy engine to connect to MySQL Database
hostname="localhost"
dbname="uc2"
uname="root"
pwd="Toor"
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))

# Convert dataframe to sql table                                   
Merge2.to_sql('patient', engine, index=False)