# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 15:11:29 2016
@author: Evgueni
Requires input files: 

"""

import argparse
import sqlite3
from datetime import datetime
import re
import csv


parser = argparse.ArgumentParser(description='''Converts healthcare tables into sqlite db.''')
#parser.add_argument('-i', '--input', help='input merged hts-count file')
parser.add_argument('-n', '--name_prefix', help='add a prefix to the db name', default='')
parser.add_argument('-b', '--build_db', help='request to build db from csv files', default='')
args = parser.parse_args()

''' enable to combine multiple csv files into one 
read_files = glob.glob("Medicare*.csv")
with open("char_disch.csv", "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            if not ln.startswith('DRF'):
                outfile.write(infile.read())
'''

inname=(args.name_prefix + 'hospitals.db3')
conn=sqlite3.connect(inname)
c=conn.cursor()

def mk_db(): #make large sample db

    inf='cdc.csv'
    c.execute('CREATE TABLE cdc (DRG_Def TEXT, Pr_Id TEXT, Pr_Name TEXT, Pr_Address TEXT, Pr_City TEXT, \
    Pr_State TEXT, Pr_Zip TEXT, Hos_ref_reg TEXT, Tot_dischar INTERGER, Avg_charges FLOAT, \
    Avg_total_pay FLOAT, Avg_medicare_pay FLOAT)')
    
    
    inpfl=open(inf, 'r')
    inpfl.readline().rstrip()
    reader=csv.reader(inpfl, delimiter=',')
    for lane in reader:
        lnl=lane
        c.execute('INSERT INTO cdc VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', \
        (lnl[0], lnl[1], lnl[2], lnl[3], lnl[4], lnl[5], lnl[6], lnl[7], lnl[8], lnl[9], lnl[10], lnl[11]))
    inpfl.close()
    conn.commit()
    
    inf='hai.csv'
    c.execute('CREATE TABLE inf (Pr_ID TEXT, Hospital TEXT, Address TEXT, City TEXT, State TEXT, ZIP TEXT, \
    County TEXT, Phone TEXT, Measure TEXT, Measure_ID, Comp_to_national TEXT, Score FLOAT, Footnote TEXT, \
    Measure_Start_Date TEXT, Measure_End_Date TEXT)')
    
    inpfl=open(inf, 'r')
    inpfl.readline().rstrip()    
    for lane in inpfl:
        ln=lane.rstrip()
        lnl=re.split('","', ln)
        lnl=[i.strip('"') for i in lnl] 
        lnl[13]=datetime.strptime(lnl[13], '%m/%d/%Y').strftime('%Y-%m-%d')
        lnl[14]=datetime.strptime(lnl[14], '%m/%d/%Y').strftime('%Y-%m-%d')   
        c.execute('INSERT INTO inf VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', \
        (lnl[0], lnl[1], lnl[2], lnl[3], lnl[4], lnl[5], lnl[6], lnl[7], lnl[8], lnl[9], lnl[10], lnl[11], \
        lnl[12], lnl[13], lnl[14]))
    inpfl.close()
    conn.commit()
    
    
    inf='consumer.csv'
    c.execute('CREATE TABLE cons (Pr_Id TEXT, Name TEXT, Address TEXT, City TEXT, \
    State TEXT, Zip TEXT, County TEXT, Phone TEXT, HCA_measure TEXT, \
    HCA_question TEXT, HCA_answ_desc TEXT, Rating FLOAT, Fnote TEXT, Answer_perc TEXT, Answer_fnote TEXT, \
    HCA_linear_val TEXT, Surv_done INTEGER, Surv_RRate FLOAT, Surv_fnote TEXT)')
    
    
    inpfl=open(inf, 'r')
    inpfl.readline().rstrip()
    reader=csv.reader(inpfl, delimiter=',')
    for lane in reader:
        lnl=lane
        if lnl[11]=='Not Applicable':
            lnl[11]=None
        c.execute('INSERT INTO cons VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', \
        (lnl[0], lnl[1], lnl[2], lnl[3], lnl[4], lnl[5], lnl[6], lnl[7], lnl[8], lnl[9], \
        lnl[10], lnl[11], lnl[12], lnl[13], lnl[14], lnl[15], lnl[16], lnl[17], lnl[18]))
    inpfl.close()
    conn.commit()
    
def prt_t_file(qr_rlt, flid): #(query result, file prefix)
    #saves query result to a file
    flnm=args.name_prefix+flid+ '.txt'
    outfile=open(flnm, 'w')
    #print(qr_rlt, file=outfile)
    for i in qr_rlt:
        ic=str(i)
        print(ic[1:-1], file=outfile)
    outfile.close()



if args.build_db!='':
    inname=(args.build_db + 'hospitals.db3')
    mk_db() #!!!!!!!!DO NOT DELETE


#get infection rates

c.execute('SELECT pr_id, SUM(score) from inf WHERE measure LIKE ("%Observed Cases%") GROUP BY pr_id HAVING SUM(score)>5')
#c.execute('SELECT state, SUM(score) from inf WHERE measure LIKE ("%Observed Cases%") GROUP BY state HAVING SUM(score)>5')
inn=c.fetchall()
prt_t_file(inn, 'inf_by_pr')

#get discharges
c.execute('SELECT pr_id, SUM(tot_dischar), SUM(avg_charges) FROM cdc GROUP BY pr_id')
#c.execute('SELECT pr_state, SUM(tot_dischar), SUM(avg_charges) FROM cdc GROUP BY pr_state')
disn=c.fetchall()
prt_t_file(disn, 'discharge_by_pr')


#c.execute('SELECT pr_id, name, SUM(rating) from cons WHERE (HCA_question LIKE ("%star rating%")) AND state="DC" GROUP BY pr_id HAVING SUM(rating)>0 ORDER by sum(rating) Desc')

#occurence of C. difficile infection
c.execute('SELECT pr_id, city, SUM(score) from inf WHERE measure LIKE ("C.diff Observed Cases%") GROUP BY pr_id HAVING SUM(score)>0 ORDER BY sum(score) DESC')
indif=c.fetchall()
prt_t_file(indif, 'difficile_by_pr')

#customer rating of DC hospitals
c.execute('SELECT pr_id, name, SUM(rating) from cons WHERE (HCA_question LIKE ("%star rating%")) AND state="DC" GROUP BY pr_id HAVING SUM(rating)>0 ORDER by sum(rating) Desc')
sdc=c.fetchall()
prt_t_file(sdc, 'rating_DC')

#processing of extracted data

ipd={}
ipc={}
for ii in inn:
    for dc in disn:
        if ii[0]==dc[0]:
            ipd.update({ii[0]:(ii[1]*100/dc[1])})       #infections per dischage
            ipc.update({ii[0]:(ii[1]*100000/dc[2])})    #infection per dollar


conn.commit()
conn.close()


        
