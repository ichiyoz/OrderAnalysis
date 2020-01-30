import os
# import MySQLdb as mdb
import simplejson
import json
import pandas as pd
from scipy import stats
import numpy as np
import xlrd

path = '/Users/yiyezhang/Documents/Data/NYPData/HF/'
        
JSON_FILE = os.path.expanduser(path+"EDDC_HF_2012_2018_ouput.json")


class DataProcess:
    #read from CSV files generated from getSourceData.ipynb
    def readFromCSV(self): 
        resultlist_cd=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/diag_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)

        resultlist_ce=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/order_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)

        resultlist_ap=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/visit_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        resultlist_demo=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/demographics_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        # return resultlist_drug, resultlist_cd, resultlist_ap, resultlist_ce, resultlist_demo, resultlist_patient,resultlist_lab 
        return resultlist_ce,resultlist_cd,resultlist_demo,resultlist_ap

    def saveDataToJson(self):

        resultlist_ce,resultlist_cd,resultlist_demo,resultlist_ap = self.readFromCSV()
        
        #create python dictionary. key is visitID (clientvisitguid)
        CE = {}
        for res in resultlist_ap.itertuples():
            
            CE[res.clientvisitguid] = dict()
            CE[res.clientvisitguid]['appt'] = dict()
            CE[res.clientvisitguid]['sex'] = 'NA'
            CE[res.clientvisitguid]['race'] = 'NA'
            CE[res.clientvisitguid]['marry'] = 'NA'
            CE[res.clientvisitguid]['language'] = 'NA'
            CE[res.clientvisitguid]['age'] = -1
            CE[res.clientvisitguid]['clientID']=res.ClientGUID
            CE[res.clientvisitguid]['dischargedisposition']=res.DischargeDCDesc
            CE[res.clientvisitguid]['dischargetime']=res.DischargeDtm
            CE[res.clientvisitguid]['admittime']=res.AdmitDtm
            CE[res.clientvisitguid]['service']=res.HospitalService

        #add demographic
        for res in resultlist_demo.itertuples():
            for pid in CE:
                if CE[pid]['clientID']==res.clientguid:
                        try:
                            CE[pid]['sex'] = res.gender
                            CE[pid]['age'] = int(res.YOB)
                            CE[pid]['race'] = res.race
                            CE[pid]['marry'] = res.mariatalstatus
                            CE[pid]['language'] = res.language
                        except:
                            pass

        #add lab orders 
        for res in resultlist_ce.itertuples():
          if res.typecode=='Diagnostic':
            if res.clientvisitguid in CE:
                
                #if timestamp already created
                if res.createdwhen in CE[res.clientvisitguid]['appt']:
                    #if order sets were used just take order set names
                    if str(res.ordersetname)!='nan' and res.ordersetname not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.ordersetname)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                    
                    #if non-order set order just label as 'Lab_order'
                    elif str(res.ordersetname)=='nan' and res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] and 'Lab_order' not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        # CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('Lab_order')
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
                    
                    #if order sets were used just take order set names
                    if str(res.ordersetname)!='nan' and res.ordersetname not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.ordersetname)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                    
                    #if non-order set order just label as 'Lab_order'
                    elif str(res.ordersetname)=='nan' and res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] and 'Lab_order' not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        # CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('Lab_order')
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
                    if res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['drug']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['drug'])
                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['user'])

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
