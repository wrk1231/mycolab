# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
import urllib2
import string
import calendar
import sqlite3
from sqlite3 import Error


def text_parser(doc_list):
    groupname_list = []
    submissiontype_list = []
    monthend_list = []
    prospectuses = []
    status = []
    
    for i in range(len(doc_list)):
        groupname_check = False
        submissiontype_check = False
        monthend_check = False
        temp_groupname = ""
        temp_submissiontype = ""
        temp_monthend = ""
        temp_status = "failed "
        
        # Retrive prospectuses of interest
        response = urllib2.urlopen(doc_list['link'].iloc[i])
        html_doc = response.read()
        
        # a. Extract header
        header = html_doc[html_doc.find('<SEC-HEADER>')+len('<SEC-HEADER>'):html_doc.find('</SEC-HEADER>')-1]
        header = header.split('\n')
    #     print(html_doc)
        # b. Extract following information from header
        # groupname
        # submission type
        # monthend
        
        for txt in header:
            if "COMPANY CONFORMED NAME:" in txt:
                temp_groupname = string.lower(txt[txt.rfind('\t')+1:].replace('\t',"").replace('\n',""))
            if "CONFORMED SUBMISSION TYPE:" in txt:
                temp_submissiontype = txt[txt.rfind('\t')+1:].replace('\t',"").replace('\n',"")
            if "FILED AS OF DATE:" in txt:
                temp_date = txt[txt.rfind('\t')+1:].replace('\t',"").replace('\n',"")
                temp_monthend = str(int(temp_date[4:6])) +'/' + str(calendar.monthrange(int(temp_date[:4]),int(temp_date[4:6]))[1]) +'/'+temp_date[:4]
            if temp_groupname != "" and temp_submissiontype != "" and temp_monthend !="":
                break
    #    print (temp_monthend, doc_list['monthend'].iloc[i])
        if temp_groupname == doc_list['groupname'].iloc[i]:
            groupname_check = True
        if temp_submissiontype == doc_list['submissiontype'].iloc[i]:
            submissiontype_check = True
        if temp_monthend == doc_list['monthend'].iloc[i]:
            monthend_check = True
        if groupname_check and submissiontype_check and monthend_check:
            groupname_list += [temp_groupname]
            submissiontype_list += [temp_submissiontype]
            monthend_list += [temp_monthend]
            status += ['processed '+temp_groupname]
            prospectuses += [html_doc[html_doc.find('<TEXT>')+len('<TEXT>'):html_doc.find('</TEXT>')-1]]
        else:
            temp_status += temp_groupname+"."
            if groupname_check is False:
                temp_status += ' groupname unmatched.'
            if submissiontype_check is False:
                temp_status += ' submissiontype unmatched.'
            if monthend_check is False:
                temp_status += ' monthend unmatched.'
            status += [temp_status]
    for line in status:
        print (line)

    df = pd.DataFrame({'groupname' : groupname_list, 'submissiontype':submissiontype_list, 'monthend':monthend_list, 'prospectus':prospectuses})
    return df   


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)    
    return None

def create_table(conn):
    """ create a table in a SQLite database """
    try:
        # command to create sql table
        create_table_sql =  """CREATE TABLE FUND(  GROUPNAME             text     NOT NULL,
                                                   SUBMISSIONTYPE        text     NOT NULL,
                                                   MONTHEND              text     NOT NULL,
                                                   PROSPECTUS            text     NOT NULL
                                                );"""

        cursor = conn.cursor()
        cursor.execute(create_table_sql)        
    except Error as e:
        print(e)

def create_fund(conn, fund):
    sql = ''' INSERT INTO FUND(GROUPNAME,SUBMISSIONTYPE,MONTHEND,PROSPECTUS)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, fund)
    return cur.lastrowid

if __name__ == '__main__':
    #access to CSV
    doc_list = pd.read_csv("RA_test.csv")
    
    # Parse text from EDGAR links
    df = text_parser(doc_list)

    RA_db = create_connection("RA.db")
    create_table(RA_db)
    with RA_db:
        for i in len(df):
            fund = (df.iloc[i]['groupname'],df.iloc[i]['submissiontype'],str(df.iloc[i]['monthend']),df.iloc[i]['prospectus'])
            create_fund(test,fund)