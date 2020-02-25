import os
import simplejson
import json
import pandas as pd
from scipy import stats
import numpy as np
import xlrd
from datetime import datetime, timedelta

path = '/Users/yiyezhang/Documents/Data/NYPData/HF/'
        
JSON_FILE = os.path.expanduser(path+"EDDC_HF_2012_2018_ouput.json")


class DataProcess:
    #read from CSV files generated from getSourceData.ipynb
    def readFromCSV(self): 
        resultlist_cd=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/diag_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)

        resultlist_ce=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/order_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)

        resultlist_ap=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/visit_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        resultlist_demo=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/demographics_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        resultlist_location=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/location_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        # return resultlist_drug, resultlist_cd, resultlist_ap, resultlist_ce, resultlist_demo, resultlist_patient,resultlist_lab 
        return resultlist_ce,resultlist_cd,resultlist_demo,resultlist_ap,resultlist_location

    def saveDataToJson(self):

        resultlist_ce,resultlist_cd,resultlist_demo,resultlist_ap,resultlist_location = self.readFromCSV()
        
        #create python dictionary. key is visitID (clientvisitguid)
        CE = {}
        for res in resultlist_ap.itertuples():
            
            CE[res.clientvisitguid] = dict()
            CE[res.clientvisitguid]['appt'] = dict()
            CE[res.clientvisitguid]['clientID']=res.clientguid
            CE[res.clientvisitguid]['dischargedisposition']=res.dischargedisposition
            CE[res.clientvisitguid]['dischargetime']=res.dischargedtm
            CE[res.clientvisitguid]['admittime']=res.admitdtm
            CE[res.clientvisitguid]['service']=res.service
            CE[res.clientvisitguid]['sex'] = ''
            CE[res.clientvisitguid]['age'] = -1
            CE[res.clientvisitguid]['race'] = ''
            CE[res.clientvisitguid]['marry'] = ''
            CE[res.clientvisitguid]['language'] = ''
            CE[res.clientvisitguid]['ethnicity'] = ''

        #add demographic
        for res in resultlist_demo.itertuples():  
            try:
                CE[res.clientvisitguid]['sex'] = res.gender
                CE[res.clientvisitguid]['age'] = res.YOB
                CE[res.clientvisitguid]['race'] = res.race
                CE[res.clientvisitguid]['marry'] = res.mariatalstatus
                CE[res.clientvisitguid]['language'] = res.language
                CE[res.clientvisitguid]['ethnicity'] = res.ethnicity1
            except:
                print('only in demo')
                        
        #add lab orders 
        for res in resultlist_ce.itertuples():
            if res.clientvisitguid in CE:
                #if timestamp already created
                if res.createdwhen in CE[res.clientvisitguid]['appt']:
                    #if order sets were used just take order set names
                    if res.typecode=='Diagnostic':
                        if str(res.ordersetname)!='nan' and res.ordersetname not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('OS'+res.ordersetname)
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                        
                        #if non-order set order just label as 'Lab_order'
                        elif str(res.ordersetname)=='nan' and res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] and 'Lab_order' not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                            # CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('Lab_order')
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                    if res.typecode=='Other':
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                        
                    #add ID of the user who placed the order
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)
                
                #if this is a new timestamp
                else:
                    CE[res.clientvisitguid]['appt'][res.createdwhen]={}
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['type']='I'
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['drug']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['lab']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['user']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['location']=[]
                    if res.typecode=='Diagnostic':
                        #if order sets were used just take order set names
                        if str(res.ordersetname)!='nan' and res.ordersetname not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('OS'+res.ordersetname)
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                        
                        #if non-order set order just label as 'Lab_order'
                        elif str(res.ordersetname)=='nan' and res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] and 'Lab_order' not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                            # CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('Lab_order')
                            CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                    if res.typecode=='Other': 
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                    #add ID of the user who placed the order
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)

        #add diagnoses
        for res in resultlist_cd.itertuples():
            if res.clientvisitguid in CE:
                if res.createdwhen in CE[res.clientvisitguid]['appt']:
                    diagcombo={}
                    diagcombo[res.typecode]=res.shortname
                    # if diagcombo not in CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']:
                    #     CE[res.clientvisitguid]['appt'][res.createdwhen]['diag'].append(diagcombo)
                    if res.icd10code not in CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['diag'].append(res.icd10code)
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)
                else:
                    CE[res.clientvisitguid]['appt'][res.createdwhen]={}
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['type']='I'
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['drug']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['lab']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['user']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['location']=[]
                    diagcombo={}
                    diagcombo[res.typecode]=res.shortname
                    # if diagcombo not in CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']:
                    #     CE[res.clientvisitguid]['appt'][res.createdwhen]['diag'].append(diagcombo)
                    if res.icd10code not in CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['diag'].append(res.icd10code)
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)

        #adding drug orders. same logic as adding lab orders (above)
        for res in resultlist_ce.itertuples():
          if res.typecode=='Medication':
            if res.clientvisitguid in CE:
                if res.createdwhen in CE[res.clientvisitguid]['appt']:
                    if res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['drug']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'])
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['user'])
                else:
                    CE[res.clientvisitguid]['appt'][res.createdwhen]={}
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['type']='I'
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['drug']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['diag']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['lab']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['user']=[]
                    CE[res.clientvisitguid]['appt'][res.createdwhen]['location']=[]
                    if res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['drug']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'])
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['user'])
        print('add locations')
        CE_location={}
        for res in resultlist_location.itertuples():
            CE_location[res.clientvisitguid]=[]
            
        for res in resultlist_location.itertuples():
            for pid in CE_location:
                if res.clientvisitguid in CE_location:
                    if [res.createdwhen,res.name] not in CE_location[res.clientvisitguid]:
                        CE_location[res.clientvisitguid].append([res.createdwhen,res.name])
            
        #adding locations
        for pid in CE:
            for date in sorted(CE[pid]['appt']): 
                for t in range(len(CE_location[pid])-1):
                    if CE_location[pid][t][0]<date and CE_location[pid][t+1][0]>date:
                        CE[pid]['appt'][date]['location'].append(CE_location[pid][t][1])
                        
                        #print(pid,date,CE[pid]['appt'][date]['location'])
                for t in range(len(CE_location[pid])):
                    if date ==CE_location[pid][t][0]:
                        CE[pid]['appt'][date]['location'].append(CE_location[pid][t][1])
                        #print(pid,date,CE[pid]['appt'][date]['location'])
                    if CE_location[pid][t][0]>date and CE_location[pid][t-1][0]<date:
                        CE[pid]['appt'][date]['location'].append(CE_location[pid][t-1][1])
                        
                        #print(pid,date,CE[pid]['appt'][date]['location'])
                    
                # print(pid,date,CE[pid]['appt'][date]['location'],CE[pid]['appt'][date])

        print('CE',len(CE))

        with open(JSON_FILE, 'w') as outfile:
            json.dump(CE, outfile, indent=2, sort_keys=True, separators=(',', ': '))

        return CE


def main():
    d = DataProcess()
    d.saveDataToJson()


def testfun():
    org = ['a', 'b']
    target = ['c', 'a', 'd', 'b']


if __name__ == '__main__':
    main()
