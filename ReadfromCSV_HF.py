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

    def readFromCSV(self): 
        resultlist_cd=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/diag_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)

        resultlist_ce=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/order_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)

        resultlist_ap=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/visit_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        resultlist_demo=pd.read_csv('/Users/yiyezhang/Documents/Data/NYPData/HF/demographics_EDDC_2012_2018.csv',sep=',',error_bad_lines=False,header=0)
        
        # return resultlist_drug, resultlist_cd, resultlist_ap, resultlist_ce, resultlist_demo, resultlist_patient,resultlist_lab 
        return resultlist_ce,resultlist_cd,resultlist_demo,resultlist_ap

    def saveDataToJson(self):

        # resultlist_drug, resultlist_cd, resultlist_ap, resultlist_ce, resultlist_demo, resultlist_patient,resultlist_lab = self.readFromDB()
        resultlist_ce,resultlist_cd,resultlist_demo,resultlist_ap = self.readFromCSV()
        
        CE = {}

        for res in resultlist_ap.itertuples():
            # p = Diagnosis(res)

            CE[res.clientvisitguid] = dict()
            CE[res.clientvisitguid]['appt'] = dict()
            CE[res.clientvisitguid]['sex'] = 'NA'
            CE[res.clientvisitguid]['race'] = 'NA'
            CE[res.clientvisitguid]['marry'] = 'NA'
            CE[res.clientvisitguid]['language'] = 'NA'
            CE[res.clientvisitguid]['age'] = -1
            CE[res.clientvisitguid]['clientID']=res.clientguid
            CE[res.clientvisitguid]['dischargedisposition']=res.dischargedisposition
            CE[res.clientvisitguid]['dischargetime']=res.dischargedtm
            CE[res.clientvisitguid]['admittime']=res.admitdtm

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

        # for res in resultlist_ap.itertuples():
        #     # ap = Appointment(res)
            
        #     CE[res.clientvisitguid]['appt']['type'] = 'ED/Inpatient Visit'
        #     CE[res.clientvisitguid]['appt']['dischargedisposition'] = res.dischargedisposition
        #     CE[res.clientvisitguid]['appt']['admittdtm'] = res.admitdtm
        #     CE[res.clientvisitguid]['appt']['dischargedtm'] = res.dischargedtm

        for res in resultlist_ce.itertuples():
          if res.typecode=='Diagnostic':
            if res.clientvisitguid in CE:
                
                if res.createdwhen in CE[res.clientvisitguid]['appt']:
                    
                    if str(res.ordersetname)!='nan' and res.ordersetname not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.ordersetname)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                        
                    elif str(res.ordersetname)=='nan' and res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] and 'Lab_order' not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        # CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('Lab_order')
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                    
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
                    
                    if str(res.ordersetname)!='nan' and res.ordersetname not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.ordersetname)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])
                        
                    elif str(res.ordersetname)=='nan' and res.name not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] and 'Lab_order' not in CE[res.clientvisitguid]['appt'][res.createdwhen]['proc']:
                        # CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append(res.name)
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'].append('Lab_order')
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'] = sorted(CE[res.clientvisitguid]['appt'][res.createdwhen]['proc'])

                    if res.userguid not in CE[res.clientvisitguid]['appt'][res.createdwhen]['user']:
                        CE[res.clientvisitguid]['appt'][res.createdwhen]['user'].append(res.userguid)
    
        for res in resultlist_cd.itertuples():
            # cd = Diagnosis(res)
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

        # for res in resultlist_lab:
        #     cd = Lab(res)
        #     if cd.visitid in CE[cd.acct]['visitIDs']:
        #         for date in CE[cd.acct]['appt']:
        #             if cd.visitid==CE[cd.acct]['appt'][date]['ID']:
        #                 if CE[cd.acct]['appt'][date]['type']=='Outpatient Visit':
        #                     labcombo={}
        #                     try:
        #                         if float(cd.result)>float(cd.low) and float(cd.result)<float(cd.high):
        #                             result='normal'
        #                         else:
        #                             result='abnormal'
        #                     except:
        #                         result='NA'
        #                     labcombo[cd.test]=result
        #                     if labcombo not in CE[cd.acct]['appt'][date]['lab']:
        #                         CE[cd.acct]['appt'][date]['lab'].append(labcombo)

        #                 else:
        #                     if cd.datetime in CE[cd.acct]['appt'][date]['withinappt']:
        #                         labcombo={}
        #                         try:
        #                             if float(cd.result)>float(cd.low) and float(cd.result)<float(cd.high):
        #                                 result='normal'
        #                             else:
        #                                 result='abnormal'
        #                         except:
        #                             result='NA'
        #                         labcombo[cd.test]=result
        #                         if labcombo not in CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['lab']:
        #                                 CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['lab'].append(labcombo)
        #                     else:
        #                         CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]={}
        #                         CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['proc']=[]
        #                         CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['drug']=[]
        #                         CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['diag']=[]
        #                         CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['lab']=[]
        #                         labcombo={}
        #                         try:
        #                             if float(cd.result)>float(cd.low) and float(cd.result)<float(cd.high):
        #                                 result='normal'
        #                             else:
        #                                 result='abnormal'
        #                         except:
        #                             result='NA'
        #                         labcombo[cd.test]=result
        #                         if labcombo not in CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['lab']:
        #                             CE[cd.acct]['appt'][date]['withinappt'][cd.datetime]['lab'].append(labcombo)


        # for res in resultlist_drug:
        #     cdrg = Drug(res)
        #     if cdrg.visitid in CE[cdrg.acct]['visitIDs']:
        #         for date in CE[cdrg.acct]['appt']:
        #             if cdrg.visitid==CE[cdrg.acct]['appt'][date]['ID']:
        #                 if CE[cdrg.acct]['appt'][date]['type']!='Outpatient Visit':
        #                     if cdrg.startdatetime in CE[cdrg.acct]['appt'][date]['withinappt']:
        #                         d = dict()
        #                         d['name'] = cdrg.drug
        #                         d['class'] = 'class'
        #                         d['prestype']=cdrg.prestype
        #                         d['status']=cdrg.stop_reason
        #                         d['startdate']=cdrg.startdate
        #                         d['enddate']=cdrg.enddate
        #                         d['sig']=cdrg.sig
        #                         d['provider']=cdrg.providerid
        #                         if d not in CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['drug']:
        #                             CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['drug'].append(d)
        #                     else:
        #                         CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]={}
        #                         CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['proc']=[]
        #                         CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['diag']=[]
        #                         CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['drug']=[]
        #                         CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['lab']=[]
        #                         d = dict()
        #                         d['name'] = cdrg.drug
        #                         d['class'] = 'class'
        #                         d['prestype']=cdrg.prestype
        #                         d['status']=cdrg.stop_reason
        #                         d['startdate']=cdrg.startdate
        #                         d['enddate']=cdrg.enddate
        #                         d['sig']=cdrg.sig
        #                         d['provider']=cdrg.providerid
        #                         if d not in CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['drug']:
        #                             CE[cdrg.acct]['appt'][date]['withinappt'][cdrg.startdatetime]['drug'].append(d)

        #                 else:
        #                     for date in CE[cdrg.acct]['appt']:
        #                         if cdrg.visitid==CE[cdrg.acct]['appt'][date]['ID']:
        #                             #match drug start by visit ID
        #                             d = dict()
        #                             d['name'] = cdrg.drug
        #                             d['class'] = 'class'
        #                             d['prestype']=cdrg.prestype
        #                             d['status']=cdrg.stop_reason
        #                             d['startdate']=date
        #                             d['enddate']=cdrg.enddate
        #                             d['sig']=cdrg.sig
        #                             d['provider']=cdrg.providerid
        #                             if d not in CE[cdrg.acct]['appt'][date]['drug']:
        #                                 CE[cdrg.acct]['appt'][date]['drug'].append(d)
                            
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
