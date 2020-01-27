from numpy import *
from copy import deepcopy
import pickle
import math
import pandas as pd
from collections import Counter
import json
from datetime import datetime, timedelta
import os
from collections import OrderedDict

path =os.path.expanduser('~/Documents/Data/NYPData/HF/')
# path = os.path.expanduser('~/Documents/Data/OMOP/')


class Structure:
    def filterData(self, data):

        eligible=[]
        for pid in data3:   
            if len(data3[pid]['appt'])>1:
                for date in data3[pid]['appt']:
                    age=int(date[0:4])-data3[pid]['age']
                    if age>0 and age<90:
                        eligible.append(pid)
                    break
        print(len(eligible))        
        
        diaglist = list()
        for pid in data:
            if len(data[pid]['appt']) != 0:
                for date in sorted(iter(data[pid]['appt'])):
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        for i in data[pid]['appt'][date]['diag']:
                            diaglist.append(i)
                            # for key, value in i.items():
                            #     diaglist.append(value)
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            for i in data[pid]['appt'][date]['withinappt'][time]['diag']:
                                for key, value in i.items():
                                    diaglist.append(value)
                                    
        c = Counter(diaglist)
        c_most_common = [item[0] for item in c.most_common(50)]
        print ('c_most_common', c_most_common)
        
        orderlist = []
        for pid in data:
            if len(data[pid]['appt']) != 0:
                for date in sorted(iter(data[pid]['appt'])):
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        for icd in range(len(data[pid]['appt'][date]['proc'])):
                            orderlist.append(str(data[pid]['appt'][date]['proc'][icd]))
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            for p in data[pid]['appt'][date]['withinappt'][time]['proc']:
                                orderlist.append(p)

        cp = Counter(orderlist)
        cp_most_common = [item[0] for item in cp.most_common(50)]
        print ('cp_most_common', cp_most_common)

        druglist = []
        for pid in data:
            if len(data[pid]['appt']) != 0:
                for date in sorted(iter(data[pid]['appt'])):
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        for icd in range(len(data[pid]['appt'][date]['drug'])):
                            druglist.append(data[pid]['appt'][date]['drug'][icd])
                            # druglist.append(data[pid]['appt'][date]['drug'][icd]['name'])
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            for p in data[pid]['appt'][date]['withinappt'][time]['drug']:
                                druglist.append(p['name'])
        cd = Counter(druglist)
        cd_most_common = [item[0] for item in cd.most_common(50)]
        print ('cd_most_common', cd_most_common)


        for pid in data:
            for date in sorted(iter(data[pid]['appt'])):
                # if data[pid]['appt'][date]['type'] == 'Emergency Room and Inpatient Visit':
                #     data[pid]['appt'][date]['type'] = 'I'
                # elif data[pid]['appt'][date]['type'] == 'Emergency Room Visit':
                #     data[pid]['appt'][date]['type'] = 'E'
                # elif data[pid]['appt'][date]['type'] == 'Inpatient Visit':
                #     data[pid]['appt'][date]['type'] = 'I'
                # elif data[pid]['appt'][date]['type'] == 'Outpatient Visit':
                #     data[pid]['appt'][date]['type'] = 'P'
                # else:
                #     print(data[pid]['appt'][date]['type'])
                    data[pid]['appt'][date]['type'] = 'I'

        readmit = {}  # temp
        readmitlist = {}  # 1 for patients who had 30-day readmission
        readmitdata = {}  # full data of patients who had 30-day readmission
        eligible=[]
        
        for pid in data:   
          if len(data[pid]['appt'])>1:
            for date in data[pid]['appt']:
                age=int(date[0:4])-data[pid]['age']
                if age>0 and age<90:
                    eligible.append(pid)
                break
        print(len(eligible))

        data2 = deepcopy(data)

        for pid in data:
            if pid in eligible:
                for date in sorted(data[pid]['appt']):
                    
                    data2[pid]['appt'][date]['diag'] = []
                    data2[pid]['appt'][date]['proc'] = []
                    data2[pid]['appt'][date]['drugclass'] = []

                    for icd in range(len(data[pid]['appt'][date]['diag'])):
                        if data[pid]['appt'][date]['diag'][icd] in c_most_common and data[pid]['appt'][date]['diag'][
                            icd] not in data2[pid]['appt'][date]['diag']: #ADD ONLY DIAGNOSIS THAT WE WANT
                            data2[pid]['appt'][date]['diag'].append(data[pid]['appt'][date]['diag'][icd])
                            
                    for icd in range(len(data[pid]['appt'][date]['proc'])):
                        if str(data[pid]['appt'][date]['proc'][icd]) in cp_most_common and str(
                                data[pid]['appt'][date]['proc'][icd]) not in data2[pid]['appt'][date]['proc']:
                            data2[pid]['appt'][date]['proc'].append(str(data[pid]['appt'][date]['proc'][icd]))

                    for icd in range(len(data[pid]['appt'][date]['drug'])):
                        if str(data[pid]['appt'][date]['drug'][icd]) in cd_most_common and str(
                                data[pid]['appt'][date]['drug'][icd]) not in data2[pid]['appt'][date]['drugclass']:
                            data2[pid]['appt'][date]['drugclass'].append(str(data[pid]['appt'][date]['drug'][icd]))

            else:
                del data2[pid]

        data3 = deepcopy(data2)
        for pid in data2:
            if pid in ('9001105983400270','9001107845700270'):
                pass
            else:
                del data3[pid]

                    
        pickle_out = open(path + 'HF_data_filtered.pickle', 'wb')
        pickle.dump(data2, pickle_out)
        pickle_out.close()
        print('len data',len(data2))

        JSON_FILE = os.path.expanduser("~/HF_data_filtered.json")

        with open(JSON_FILE, 'w') as outfile:
            json.dump(data2, outfile, indent=2, sort_keys=True, separators=(',', ': '))

        # pickle_out = open(path + 'HF_readmit.pickle', 'wb')
        # pickle.dump(readmitlist, pickle_out)
        # pickle_out.close()

        # pickle_out = open(path + 'HF_readmit_data.pickle', 'wb')
        # pickle.dump(readmitdata, pickle_out)
        # pickle_out.close()
        # print(len(data2))
        return data2

    def getNode(self, data):

        diaglist = list()
        for pid in data:
            if len(data[pid]['appt']) != 0:
                for date in data[pid]['appt']:
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        if len(data[pid]['appt'][date]['diag']) != 0 and data[pid]['appt'][date][
                            'diag'] not in diaglist:
                            diaglist.append(data[pid]['appt'][date]['diag'])
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            if len(data[pid]['appt'][date]['withinappt'][time]['diag']) != 0 and \
                                    data[pid]['appt'][date]['withinappt'][time]['diag'] not in diaglist:
                                diaglist.append(data[pid]['appt'][date]['withinappt'][time]['diag'])

        orderlist = list()
        for pid in data:
            if len(data[pid]['appt']) > 0:
                for date in data[pid]['appt']:
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        if len(data[pid]['appt'][date]['proc']) != 0 and str(
                                data[pid]['appt'][date]['proc']) not in orderlist:
                            orderlist.append(str(data[pid]['appt'][date]['proc']))
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):

                            if len(data[pid]['appt'][date]['withinappt'][time]['proc']) != 0 and \
                                    data[pid]['appt'][date]['withinappt'][time]['proc'] not in orderlist:
                                orderlist.append(data[pid]['appt'][date]['withinappt'][time]['proc'])

        druglist = list()
        for pid in data:
            if len(data[pid]['appt']) != 0:
                for date in data[pid]['appt']:
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        if len(data[pid]['appt'][date]['drugclass']) != 0 and data[pid]['appt'][date][
                            'drugclass'] not in druglist:
                            druglist.append(data[pid]['appt'][date]['drugclass'])
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            if len(data[pid]['appt'][date]['withinappt'][time]['drugclass']) != 0 and \
                                    data[pid]['appt'][date]['withinappt'][time]['drugclass'] not in druglist:
                                druglist.append(data[pid]['appt'][date]['withinappt'][time]['drugclass'])

        print('getNode1', datetime.now())
        nodedesc = dict()
        for t in range(0, len(orderlist)):
            if orderlist[t] != '':
                nodedesc[str(orderlist[t])] = 'O' + str(t)
                # nodedesc['O' + str(t)] = orderlist[t]
        nodedesc['O_NR'] = 'O_NR'
        for x in range(0, len(druglist)):
            if druglist[x] != '':
                nodedesc[str(druglist[x])] = 'M' + str(x)
                # nodedesc['M' + str(x)] = druglist[x]
        nodedesc['M_NR'] = 'M_NR'
        for s in range(0, len(diaglist)):
            if diaglist[s] != '':
                nodedesc[str(diaglist[s])] = 'D' + str(s)
                # nodedesc['D' + str(s)] = diaglist[s]
        nodedesc['D_NR'] = 'D_NR'
        print('getNode2', datetime.now())

        print ('node created')
        pickle_out = open(path + 'HF_node.pickle', 'wb')
        pickle.dump(nodedesc, pickle_out)
        pickle_out.close()
        # with open("data/nodedesc.json",'w') as outfile:
        #     json.dump(nodedesc, outfile)
        return nodedesc

    def getV(self, nodedesc, data):
        for pid in data:
            if len(data[pid]['appt']) != 0:

                for date in data[pid]['appt']:
                    if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                        if len(data[pid]['appt'][date]['diag']) == 0:
                            data[pid]['appt'][date]['diag'] = 'D_NR'
                        else:
                            data[pid]['appt'][date]['diag'] = nodedesc[str(data[pid]['appt'][date]['diag'])]
                        if len(data[pid]['appt'][date]['proc']) == 0:
                            data[pid]['appt'][date]['proc'] = 'O_NR'
                        else:
                            data[pid]['appt'][date]['proc'] = nodedesc[str(data[pid]['appt'][date]['proc'])]
                        if len(data[pid]['appt'][date]['drugclass']) == 0:
                            data[pid]['appt'][date]['drugclass'] = 'M_NR'
                        else:
                            data[pid]['appt'][date]['drugclass'] = nodedesc[str(data[pid]['appt'][date]['drugclass'])]
                    elif len(data[pid]['appt'][date]['withinappt']) > 0:
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            if len(data[pid]['appt'][date]['withinappt'][time]['diag']) == 0:
                                data[pid]['appt'][date]['withinappt'][time]['diag'] = 'D_NR'
                            else:
                                data[pid]['appt'][date]['withinappt'][time]['diag'] = nodedesc[
                                    str(data[pid]['appt'][date]['withinappt'][time]['diag'])]
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            if len(data[pid]['appt'][date]['withinappt'][time]['proc']) == 0:
                                data[pid]['appt'][date]['withinappt'][time]['proc'] = 'O_NR'
                            else:
                                data[pid]['appt'][date]['withinappt'][time]['proc'] = nodedesc[
                                    str(data[pid]['appt'][date]['withinappt'][time]['proc'])]
                        for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                            if len(data[pid]['appt'][date]['withinappt'][time]['drugclass']) == 0:
                                data[pid]['appt'][date]['withinappt'][time]['drugclass'] = 'M_NR'
                            else:
                                data[pid]['appt'][date]['withinappt'][time]['drugclass'] = nodedesc[
                                    str(data[pid]['appt'][date]['withinappt'][time]['drugclass'])]

        print('getV1', datetime.now())
        visitlist = dict()
        w = 0
        w2 = 0
        for pid in data:
            for date in sorted(data[pid]['appt']):
                if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                    visitlist[w] = str(data[pid]['appt'][date]['type']) + str(data[pid]['appt'][date]['diag']) + str(
                        data[pid]['appt'][date]['proc']) + str(data[pid]['appt'][date]['drugclass'])
                    # if data[pid][date]['zipcode']=='' or site(data[pid][date]['zipcode'])==float:
                    #     visitlist[w].append('NA')
                    # else:
                    #     visitlist[w].append(data[pid][date]['zipcode'])
                    w = w + 1
                elif len(data[pid]['appt'][date]['withinappt']) > 0:
                    for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                        visitlist[w2] = str(data[pid]['appt'][date]['type']) + str(
                            data[pid]['appt'][date]['withinappt'][time]['diag']) + str(
                            data[pid]['appt'][date]['withinappt'][time]['proc']) + str(
                            data[pid]['appt'][date]['withinappt'][time]['drugclass'])
                        w2 += 1

        print('getV2', datetime.now())

        d_visitlist = list()
        for w in range(0, len(visitlist)):
            if visitlist[w] not in d_visitlist:
                d_visitlist.append(visitlist[w])

        print('getV3', datetime.now())

        Vdesc = dict()
        for t in range(0, len(d_visitlist)):
            if d_visitlist[t] != '':
                Vdesc[str(d_visitlist[t])] = 'V' + str(t)
        Vdesc['D_NRO_NRM_NR'] = 'D_NRO_NRM_NR'
        print('getV4', datetime.now())

        pickle_out = open(path + 'HF_V.pickle', 'wb')
        pickle.dump(Vdesc, pickle_out)
        pickle_out.close()

        # with open("data/Vdesc.json", 'w') as outfile:
        #     json.dump(Vdesc, outfile)

        pickle_out = open(path + 'HF_V_data.pickle', 'wb')
        pickle.dump(data, pickle_out)
        pickle_out.close()

        return data, Vdesc

    def getSeq(self, data, Vdesc):
        VT = dict()
        tempDT = dict()

        for pid in data:
            t = Structure()
            VT[pid] = list()
            # tempDT[pid] = list()
            # VT[pid].append('start')
            # tempDT[pid].append('start')
            for date in sorted(data[pid]['appt']):
                if 'withinappt' not in data[pid]['appt'][date] or len(data[pid]['appt'][date]['withinappt']) == 0:
                    # tempDT[pid].append(data[pid]['appt'][date]['actualdate'])
                    VT[pid].append(Vdesc[str(data[pid]['appt'][date]['type']) + str(
                        data[pid]['appt'][date]['diag']) + str(data[pid]['appt'][date]['proc']) + str(
                        data[pid]['appt'][date]['drugclass'])])
                elif len(data[pid]['appt'][date]['withinappt']) > 0:
                    for time in sorted(iter(data[pid]['appt'][date]['withinappt'])):
                        # tempDT[pid].append(data[pid]['appt'][date]['withinappt'][time]['actualtime'])
                        VT[pid].append(Vdesc[str(data[pid]['appt'][date]['type']) + str(
                            data[pid]['appt'][date]['withinappt'][time]['diag']) + str(
                            data[pid]['appt'][date]['withinappt'][time]['proc']) + str(
                            data[pid]['appt'][date]['withinappt'][time]['drugclass'])])

        print('getSeq1', datetime.now())

        # for pid in VT:
        #     if len(VT[pid])>1:
        #         print (pid, data[pid]['location'], VT[pid])
        print('getSeq2', datetime.now())
        # VT=self.findRepeats(VT)
        print (len(VT))
        pickle_out = open(path + 'HF_VT.pickle', 'wb')
        pickle.dump(VT, pickle_out)
        pickle_out.close()
        return VT, tempDT

    def findRepeats(self, VT):
        seen = {}

        uniq = {}
        # IF GET ACTUAL REPEATS
        for pid in VT:
            seen[pid] = set()
            uniq[pid] = {}
            for x in range(len(VT[pid])):
                if VT[pid][x] not in seen[pid]:
                    uniq[pid][VT[pid][x]] = 0
                    uniq[pid][VT[pid][x]] = 1 + uniq[pid][VT[pid][x]]
                    seen[pid].add(VT[pid][x])
                else:
                    uniq[pid][VT[pid][x]] = uniq[pid][VT[pid][x]] + 1
                    VT[pid][x] = str(VT[pid][x]) + '_' + str(uniq[pid][VT[pid][x]])

        # #IF ONLY LABEL 
        # for pid in VT:
        #     seen[pid]=set()
        #     uniq[pid]={}
        #     for x in range(len(VT[pid])):
        #         if VT[pid][x] not in seen[pid]:
        #             uniq[pid][VT[pid][x]]=0
        #             uniq[pid][VT[pid][x]]=1+uniq[pid][VT[pid][x]]
        #             seen[pid].add(VT[pid][x])
        #         else:
        #             uniq[pid][VT[pid][x]]=uniq[pid][VT[pid][x]]
        #             VT[pid][x]=str(VT[pid][x])+'_'+str(uniq[pid][VT[pid][x]])
        return VT


def main():
    d = Structure()

    fh = open(path + "EDDC_HF_ouput.json", 'r')
    data = json.load(fh)
    
    # SEE JUPITER printDiscofile

    # pickle_out = open(path+'HF_data.pickle','rb')
    # data = pickle.load(pickle_out)
    # pickle_out.close()
    print ('data loaded')
    print ('num patient',len(data))
    
    # data2 = d.filterData(data)
    # print ('data filtered')
    
    pickle_out = open(path + 'data_18_89_EDDC_2012_2018_filtered.pickle', 'rb')
    data2 = pickle.load(pickle_out)
    pickle_out.close()
    # fh = open(path + "HF_data_filtered_OS.json", 'r')
    # data2 = json.load(fh)

    nodedesc = d.getNode(data2)
    pickle_out = open(path + 'HF_node.pickle', 'rb')
    nodedesc = pickle.load(pickle_out)
    pickle_out.close()

    data2, Vdesc = d.getV(nodedesc, data2)
    pickle_out = open(path + 'HF_V.pickle', 'rb')
    Vdesc = pickle.load(pickle_out)
    pickle_out.close()
    VT, tempDT = d.getSeq(data2, Vdesc)  ##LINE 226!!!!!

    
    # pickle_out = open(path+'_V_data.pickle','rb')
    # data2 = pickle.load(pickle_out)
    # pickle_out.close()
    # print ('V',Vdesc)

    #data2=d.filterV(data2,nodedesc)
    

if __name__ == '__main__':
    main()
