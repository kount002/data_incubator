# -*- coding: utf-8 -*-
"""
Created on Fri Apr  8 12:42:50 2016

@author: Evgueni
"""

import sqlite3
import argparse


def prt_t_file(qr_rlt, flid): #(query result, file prefix)
    #saves query result to a file
    flnm=flid +'_'+ args.name_prefix
    outfile=open(flnm, 'x')
    
    print(qr_rlt, file=outfile)
    
    outfile.close()

parser = argparse.ArgumentParser(description='''
Convert hts-count merged tables into sqlite db.
DB organized as a collection of tables with one table per sample.
 A table contains PRIMARY KEY column "gene" and float value column.
''')
parser.add_argument('-i', '--input', help='input merged hts-count file', required=True)
parser.add_argument('-n', '--name_prefix', help='add a prefix to the output files', default='query_result.txt')
#parser.add_argument('-o', '--output', help='output file', default='counting_out.txt')
#parser.add_argument('-c', '--cutoff', help='reads per gene to remove', default=0)
args = parser.parse_args()

file=args.input
conn=sqlite3.connect(file)
c=conn.cursor()

while True:
    inp=input('Enter query or exit \n')
    
    if inp=='exit':
        break
    else:
        try:        
            c.execute(inp)
            output=c.fetchall()
            for i in output:
                ic=str(i)
                print(ic[1:-1])
               
            print("\n Total rows: ", len(output))
            while True:
                inp2=input('Save (y/n)? \n')
                if inp2=='y':
                    print(' query and data saving routine')
                    inp3=input('Enter file name: \n')
                    inwf=open(inp3, 'w')
                    print(inp, file=inwf)
                    for i in output:
                        ic=str(i)
                        print(ic[1:-1], file=inwf)
                    inwf.close()
                    break
                    
                else:
                    break
        except sqlite3.Error as e:
            print('Error, try again: ', e)
            #check to display sql error msg
        
            
            
        
        
