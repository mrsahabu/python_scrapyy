#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 01:20:42 2019

@author: sahab
"""
import argparse
import requests
from bs4 import BeautifulSoup 
from find_job_titles import Finder
from geotext import GeoText
import sqlite3

def run(args):
    scrapper = Scrapper(args)
    url = scrapper.get_url(args.url)
    content = scrapper.make_http_request(url)
    parser =  HtmlParser(content,args.debug)
    parser.read_job_titles_file(args.file_name)
    parser.find_all_links()
    parser.get_information()
    
    model = Model(args.db_name,args.debug)
    all_data = model.get_all_jobs(args.db_name)
    tupled_list = [tuple(el) for el in parser.scraped_list]
    new_rows = parser.get_uncommon(all_data,tupled_list)
    for row in new_rows:
        model.insert_jobs(row[0],row[1],row[2],args.db_name)
    all_data = model.get_all_jobs(args.db_name)        
    print('{}'.format(all_data)) 
    print('Finised...Total No of Rows stored {}'.format(len(new_rows)))
    


class HtmlParser():
    def __init__(self,url,debug):
        self.soup=BeautifulSoup(url,"html.parser")
        self.list_of_link_text=[]
        self.jobs_list = []
        self.scraped_list = []
        self.debug = debug     
        
    def find_all_links(self):
        for links in self.soup.find_all('a',attrs={'class':'storylink'}):
            self.list_of_link_text.append(links.text)
                
    def read_job_titles_file(self,path_to_file):
        try:
            self.jobs_list = [line.rstrip('\n').lower() for line in open(path_to_file)]
        except:
            print('File not found error');
    
    def get_information(self):
        finder = Finder(self.jobs_list)
        for strr in self.list_of_link_text:
            company_name = self.get_company(strr)
            job_name = self.get_job(finder,strr.lower())
            location = self.get_location(strr)
            self.scraped_list.append([company_name,job_name,location])
            if self.debug:
                print('Company Name: {}, Job: {}, Location: {}'.format(company_name,job_name,location))        
        
    def get_location(self, strr):
        location = GeoText(strr)
        if len(location.cities) > 0:
            location = location.cities[0]
        else:
            location = 'None'
        return location    
       
    def get_company(self,strr):
       return strr.split(' ',1)[0]
   
    def get_job(self,finder,strr):
        
       assumed_spliting_words=['hiring', 'looking for']
       job_name = ""
       try:
         job_name = finder.findall(strr)
         job_name = job_name[0][2]
       except:
           try:
               strr = strr.lower()
               for word in assumed_spliting_words:
                   if strr.find(word) != -1:
                       job_name = strr.split(word,1)[1] 
           except:
               job_name = None
       return job_name  

    def get_uncommon(self,all_data,tupled_list):
        set_db_data = set(all_data)
        set_tupled_list= set(tupled_list)
        return list(set_tupled_list - set_db_data)

class Model:
    def __init__(self,db_name,debug):
        self.create_database_and_table(db_name)
        self.debug = debug
 
    def create_database_and_table(self,db_name):
        try:
            db = sqlite3.connect(db_name)
            cursor = db.cursor()
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              jobs(id INTEGER PRIMARY KEY, name TEXT, title TEXT, location TEXT)''')
            db.commit()
   
              
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()
   
    def get_db_connection(self,db_name):
       db = sqlite3.connect(db_name)
       if self.debug:
           print('Database/table connection created')
       return db,db.cursor()
         
    def insert_jobs(self,name,title,loc,db_name):
       try:
           db = sqlite3.connect(db_name)
           cursor = db.cursor()
           cursor.execute('''INSERT INTO jobs(name, title, location)
                  VALUES(?,?,?)''', (name,title,loc))            
           db.commit() 
       except Exception as e:
            db.rollback()
            raise e
       finally:
            db.close()
            
    def get_all_jobs(self,db_name):
        try:
            db,cursor = self.get_db_connection(db_name)
            cursor.execute('SELECT name,title,location FROM jobs')
        except:
            print('Get all jobs error')
        return cursor.fetchall()
 
    def get_latest_job(self,db_name):
        try:        
            db,cursor = self.get_db_connection(db_name)
            cursor.execute('SELECT * FROM jobs ORDER BY id DESC LIMIT 1')
            row = cursor.fetchall()
            if(self.debug):
                print('Latest row: {} '.format(row))
               
        except:
            print('Get latest job error')            
        return row

class Scrapper():
    def __init__(self,args):
        self.debug = args.debug
        self.db_name = args.db_name
        self.url= ""

        
    def get_url(self,path_to_file):
          url = ""  
          try:
            f = open(path_to_file,'r')
            url = f.read()
            self.url=url[:url.index("\n")]
            f.close()
            return self.url

          except:
            print('url file not found')
    def make_http_request(self,url):
        request = requests.get(url,timeout=10)
        if self.debug:
            print('Request status code: {}'.format(request.status_code))
        if request.status_code == 200:
            request.connection.close()
            return request.content
        else:
            print('Http error')

    


 
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file_name", default='titles_combined.txt',
                    help="file contains Job Titles")
    parser.add_argument("-db", "--db_name", default='scrapyydb',
                    help="database name")
    parser.add_argument("-u", "--url", default='url.txt',
                    help="file contains url")
    parser.add_argument("-d", "--debug", default=False)
    args = parser.parse_args()
    run(args)
       
