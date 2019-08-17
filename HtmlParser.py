
import argparse
import requests
from bs4 import BeautifulSoup 
from find_job_titles import Finder
from geotext import GeoText
import sqlite3

def run(args):
    main = Main(args)
    url = main.get_url(args.url)
    content = main.make_http_request(url)
    scrapper =  HtmlParser(content,args.debug)
    scrapper.read_job_titles_file(args.file_name)
    scrapper.find_all_links()
    scrapper.get_information()
    model = Model(args.db_name,args.debug)
    last_row = model.get_latest_job(args.db_name)
    latest_row = scrapper.scraped_list[-1]
    if len(last_row) != 0:
        if   last_row [0][1] == latest_row[0] and last_row [0][2] == latest_row[1] and last_row [0][3] == latest_row[2]:
               print('No new entry')
    else:  
        for row in scrapper.scraped_list:
            model.insert_jobs(row[0],row[1],row[2],args.db_name)
            
    print('Finised')

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
            cursor.execute('SELECT * FROM jobs')
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

class Main():
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
        request = requests.get(url)
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
