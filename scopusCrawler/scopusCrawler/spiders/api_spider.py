import scrapy
import time
import calendar
import datetime
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DontCloseSpider
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import fromstring, tostring

from ..items import *

prefix_dict = {'dc' : 'http://purl.org/dc/elements/1.1/', 'ns0' : 'http://www.w3.org/2005/Atom', 
    'ns1' : 'http://a9.com/-/spec/opensearch/1.1/', 'ns2' : 'http://prismstandard.org/namespaces/basic/2.0/'}

countries = [
    'United States',
    'China',
    'United Kingdom',
    'Germany',
    'Japan',
    'France',
    'Canada',
    'Italy',
    'India',
    'Australia',
    'Spain',
    'Russian Federation',
    'South Korea',
    'Netherlands',
    'Brazil',
    'Switzerland',
    'Sweden',
    'Poland',
    'Taiwan',
    'Iran',
    'Turkey',
    'Belgium',
    'Denmark',
    'Israel',
    'Austria',
    'Finland',
    'Norway',
    'Mexico',
    'Portugal',
    'South Africa',
    'Malaysia',
    'Czech Republic',
    'Greece',
    'Hong Kong',
    'Singapore',
    'New Zealand'
]

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']
          
doctypes = ['ar', 'cp', 're', 'ch', 'no', 'le', 'bk', 'sh', 'ed', 'er', 'cr', 'tb', 'rp', 'dp', 'ab', 'bz']

oas = ['publisherfullgold', 'publisherhybridgold', 'publisherfree2read']

subject_dict = {
    'AGRI': 1,
    'ARTS': 2,
    'BIOC': 3,
    'BUSI': 4,
    'CENG': 5,
    'CHEM': 6,
    'COMP': 7,
    'DECI': 8,
    'DENT': 9,
    'EART': 10,
    'ECON': 11,
    'ENER': 12,
    'ENGI': 13,
    'ENVI': 14,
    'HEAL': 15,
    'IMMU': 16,
    'MATE': 17,
    'MATH': 18,
    'MEDI': 19,
    'NEUR': 20,
    'NURS': 21,
    'PHAR': 22,
    'PHYS': 23,
    'PSYC': 24,
    'SOCI': 25,
    'VETE': 26,
    'MULT': 27,
}

chars = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

api_keys = ['64b192ed674a6c372664826c5a5dce03', '549062e2e56c1b8051439bbfaff57084']

class ScopusSpider(scrapy.Spider):
    name = 'scopus_api'
    itertag = 'entry'
    
    def start_requests(self):
        self.headers =  {
            'Accept': 'application/xml',
        }
        
        self.api_count = 0
        self.results = 0
        self.dates = ['1980', '1990', '2000', '2006', '2010', '2014', '2018', '2018', '2020', '2022']
        self.subject = 'AGRI'
        #self.api_key = '549062e2e56c1b8051439bbfaff57084'
        #self.api_key = '64b192ed674a6c372664826c5a5dce03'
        self.articles_count = 0
        
        date = 2022
                
        self.volumes = [str(i) for i in range(500)] + chars + ['ii', 'iii', 'iv', 'vi', 'xi', 'ix', 'xii', 'xiv', 'xvi']
        self.volumes += months
        self.volumes += [f"{date}-{month}" for month in months]
        
        #for m in months:
        #yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query=a&date={date}",
           # headers = self.headers, callback = self.parse_doctype, errback=self.errback, meta = {"date": date, "query": ""})
        #for subj in subject_dict:
            #self.subject = subj
        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query=all&date={date}",
                headers = self.headers, callback = self.parse_doctype, errback=self.errback, meta = {"date": date, "query": ""}, dont_filter=True)
            
            #self.results = 0
            #self.logger.info('subj %s results %s', self.subject, failure.code)
    
    def errback(self, failure):
        self.logger.info('failureeeeeeeee %s', failure.code)
 
        if failure.code == "429":
            self.api_count += 1    

    def parse_doctype(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"start total result {total_results} url {url}")
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            for doctype in doctypes:
                yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query=DOCTYPE%28+{doctype}+%29&date={date}",
                    headers = self.headers, callback = self.parse_cited_by_helper, errback=self.errback, meta = {"date": date, "query": f"DOCTYPE%28+{doctype}+%29"}, dont_filter=True)
            
            #yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query=NOT+DOCTYPE%28+{'%29+AND+NOT+DOCTYPE%28'.join(doctypes)}%29&date={date}&sort=citedby-count",
                 #   headers = self.headers, callback = self.parse_cited_by, errback=self.errback, meta = {"date": date, "query": f"NOT+DOCTYPE%28+{'%29+AND+NOT+DOCTYPE%28'.join(doctypes)}%29"})
                    
            self.logger.info(f"parse_access")  


    def parse_cited_by_helper(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"start total result {total_results} url {url}")
        
        if total_results == 0:
            return

        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return      
        else:
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}&date={date}&sort=citedby-count",
                headers = self.headers, callback = self.parse, errback=self.errback, meta = {"date": date, "query": query}, dont_filter=True)  
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}&date={date}&sort=citedby-count&start=25",
                headers = self.headers, callback = self.parse, errback=self.errback, meta = {"date": date, "query": query}, dont_filter=True)    
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}&date={date}&sort=citedby-count&start=50",
                headers = self.headers, callback = self.parse_cited_by, errback=self.errback, meta = {"date": date, "query": query}, dont_filter=True)     


    def parse_cited_by(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"afff total result {total_results} url {url}")

        if total_results == 0:
            return

        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            entry = tree.find(f".//{{{prefix_dict['ns0']}}}entry")
            citedby_count = int(entry.find(f".//{{{prefix_dict['ns0']}}}citedby-count").text)
            self.logger.info(f"citedby_count {citedby_count}")
            
            if citedby_count != 0:
                if citedby_count > 200:
                    step = citedby_count // 200
            
                    for volume_ind in range(0, step + 1):
                        if volume_ind == step:
                            right = citedby_count + 1
                        else:
                            right = (volume_ind + 1) * 200
                            
                        str_lhs = f"CITEDBY%28+{'+OR+'.join([str(i) for i in range(volume_ind * 200, right)])}%29"
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                            headers = self.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": volume_ind * 200, "rhs_count": right,  
                            "query": query}, dont_filter=True)
                 
                else:
                    if citedby_count == 2:
                        midle = int(citedby_count // 2)
                    else:  
                        midle = int(citedby_count // 3)
                        
                    if citedby_count == 1:
                        str_lhs = "CITEDBY%28+0+%29"
                        str_rhs = "CITEDBY%28+1+%29"
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                            headers = self.headers, callback = self.parse_access, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                            headers = self.headers, callback = self.parse_access, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)   
                            
                    else:  
                        str_lhs = f"CITEDBY%28+{'+OR+'.join([str(i) for i in range(0, midle)])}%29"
                        str_rhs = f"CITEDBY%28+{'+OR+'.join([str(i) for i in range(midle, citedby_count + 1)])}%29"
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                        headers = self.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": 0, "rhs_count": midle, "query": query}, dont_filter=True)
                        
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                        headers = self.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": midle, "rhs_count": citedby_count + 1, "query": query}, dont_filter=True)     
            else:
                yield from self.parse_access(response)  
                
   
    def parse_cited_by_binary_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"afff total result {total_results} url {url}")
        
        if total_results == 0:
            return
     
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            if 'lhs_count' in response.meta:               
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 3)
                                   
                if response.meta['rhs_count'] - response.meta['lhs_count'] < 3:
                    str_lhs = f"CITEDBY%28+{response.meta['lhs_count']}+%29"
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                            headers = self.headers, callback = self.parse_access, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)  
                    
                    if response.meta['rhs_count'] - response.meta['lhs_count'] == 1:
                        str_rhs = f"CITEDBY%28+{response.meta['lhs_count'] + 1}+%29"
                        
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                            headers = self.headers, callback = self.parse_access, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)  
                            
                    elif response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                        midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 2)
                        str_rhs = f"CITEDBY%28+{'+OR+'.join([str(i) for i in range(midle, response.meta['rhs_count'])])}%29"
                                                
                        yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                            headers = self.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": midle, "rhs_count": response.meta['rhs_count'],"query": query}, dont_filter=True)
                           
                else:  
                    str_lhs = f"CITEDBY%28+{'+OR+'.join([str(i) for i in range(response.meta['lhs_count'], midle)])}%29"
                    str_rhs = f"CITEDBY%28+{'+OR+'.join([str(i) for i in range(midle, response.meta['rhs_count'])])}%29"
                
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                        headers = self.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": response.meta['lhs_count'], "rhs_count": midle, "query": query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                        headers = self.headers, callback = self.parse_cited_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": midle, "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)     
    
    
    def parse_access(self, response, cited = None, rhs_cited = None, volume = False):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"start total result {total_results} url {url}")
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            if cited is None:
                str_access = lambda access: f"{query}+AND+OPENACCESS%28+{access}+%29"
            elif volume == True:
                str_access = lambda access: f"{query}+AND+OPENACCESS%28+{access}+%29+AND+VOLUME%28+{'+OR+'.join(self.volumes[cited : rhs_cited])}%29"
            else:
                str_access = lambda access: f"{query}+AND+OPENACCESS%28+{access}+%29+AND+CITEDBY%28+{'+OR+'.join([str(i) for i in range(cited, rhs_cited)])}%29"
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={str_access(1)}&date={date}",
                headers = self.headers, callback = self.parse_oas, errback=self.errback, meta = {"date": date, "query": str_access(1)}, dont_filter=True)
            
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={str_access(0)}&date={date}",
                headers = self.headers, callback = self.parse_authfirst, errback=self.errback, meta = {"date": date, "query": str_access(0)}, dont_filter=True)
                    
        self.logger.info(f"parse_access")
  
    def parse_oas(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"start total result {total_results} url {url}")
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            for oa in oas:
                yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+OA%28{oa}%29&date={date}",
                    headers = self.headers, callback = self.parse_authfirst, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+OA%28{oa}%29"}, dont_filter=True)
                  
        self.logger.info(f"parse_access")
      
  
    def parse_authfirst(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"start total result {total_results} url {url}")
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            midle = int(len(chars) // 3)
            str_lhs = f"AUTHFIRST%28{'+OR+'.join(chars[0 : midle])}%29"
            str_rhs = f"AUTHFIRST%28{'+OR+'.join(chars[midle: len(chars)])}%29"
                
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                headers = self.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"date": date, "lhs_count": 0, "rhs_count": midle, "query": query}, dont_filter=True)
                    
            yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                headers = self.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"date": date, "lhs_count": midle, "rhs_count": len(chars), "query": query}, dont_filter=True) 
                    
            self.logger.info(f"parse_access")
            
            
    def parse_authfirst_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_authfirst_rec {total_results} url {url}")
        
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            if 'lhs_count' in response.meta:
                midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 2)
        
                if response.meta['rhs_count'] - response.meta['lhs_count'] < 3:
                    str_lhs = f"AUTHFIRST%28{chars[response.meta['lhs_count']]}%29"
                    
                    if response.meta['rhs_count'] - response.meta['lhs_count'] == 1:
                        str_rhs = f"AUTHFIRST%28{chars[response.meta['lhs_count'] + 1]}%29"
                        rhs_callback = self.parse_volume  
                    elif response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                        str_rhs = f"AUTHFIRST%28+{'+OR+'.join(chars[midle : response.meta['rhs_count']])}%29"
                        rhs_callback = self.parse_authfirst_rec
                        
                    self.logger.info(f"response.meta['rhs_count'] - response.meta['lhs_count'] == 2:")
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                        headers = self.headers, callback = self.parse_volume, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                        headers = self.headers, callback = rhs_callback, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)   
                        
                else:  
                    self.logger.info(f"response.meta['rhs_count'] - response.meta['lhs_count'] not 2:")
                    str_lhs = f"AUTHFIRST%28{'+OR+'.join(chars[response.meta['lhs_count'] : midle])}%29"
                    str_rhs = f"AUTHFIRST%28{'+OR+'.join(chars[midle : response.meta['rhs_count']])}%29"
                
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                        headers = self.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"date": date, "lhs_count": response.meta['lhs_count'], "rhs_count": midle, "query": query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                        headers = self.headers, callback = self.parse_authfirst_rec, errback=self.errback, meta = {"date": date, "lhs_count": midle, "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)  

     
    def parse_volume(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_volume {total_results} url {url}")

        if 'volume' not in response.meta:
            volume = 1
        else:
            volume = int(response.meta['volume']) + 10
            
        if total_results == 0:
            return
         
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            midle = len(self.volumes) // 7
            
            for volume_ind in range(0, 7):
                if volume_ind == 6:
                    right = len(self.volumes) - 1
                else:
                    right = (volume_ind + 1) * midle
                    
                str_lhs = f"VOLUME%28+{'+OR+'.join(self.volumes[volume_ind * midle : right])}%29"
                
                yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                    headers = self.headers, callback = self.parse_volume_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": volume_ind * midle, "rhs_count": right, "query": query}, dont_filter=True)

            self.logger.info(f"parse_access")


    def parse_volume_by_binary_rec(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
        
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"afff total result {total_results} url {url}")
        
        if total_results == 0:
            return
     
        if total_results <= 5000:
            yield from self.parse_start(response)
            self.logger.info(self.results)
            return
        else:
            if 'lhs_count' in response.meta:
                if response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                    midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 2)
                else:
                    midle = int(response.meta['lhs_count'] + (response.meta['rhs_count'] - response.meta['lhs_count']) // 3)
                                   
                if response.meta['rhs_count'] - response.meta['lhs_count'] < 3:
                    str_lhs = f"VOLUME%28+{response.meta['lhs_count']}+%29"
                    
                    if response.meta['rhs_count'] - response.meta['lhs_count'] == 1:
                        str_rhs = f"VOLUME%28{chars[response.meta['lhs_count'] + 1]}%29"
                        rhs_callback = self.parse_start  
                    elif response.meta['rhs_count'] - response.meta['lhs_count'] == 2:
                        str_rhs = f"VOLUME%28+{'+OR+'.join(self.volumes[midle : response.meta['rhs_count']])}%29"
                        rhs_callback = self.parse_volume_by_binary_rec
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                        headers = self.headers, callback = self.parse_start, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_lhs}"}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                        headers = self.headers, callback = rhs_callback, errback=self.errback, meta = {"date": date, "query": f"{query}+AND+{str_rhs}"}, dont_filter=True)   
                        
                else:  
                    str_lhs = f"VOLUME%28{'+OR+'.join(self.volumes[response.meta['lhs_count'] : midle])}%29"
                    str_rhs = f"VOLUME%28{'+OR+'.join(self.volumes[midle : response.meta['rhs_count']])}%29"
                
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_lhs}&date={date}",
                        headers = self.headers, callback = self.parse_volume_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": response.meta['lhs_count'], "rhs_count": midle, "query": query}, dont_filter=True)
                    
                    yield Request(f"https://api.elsevier.com/content/search/scopus?apiKey={api_keys[self.api_count]}&subj={self.subject}&query={query}+AND+{str_rhs}&date={date}",
                        headers = self.headers, callback = self.parse_volume_by_binary_rec, errback=self.errback, meta = {"date": date, "lhs_count": midle, "rhs_count": response.meta['rhs_count'], "query": query}, dont_filter=True)   


    def parse_start(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        url = response.request.url
        query =  response.meta['query']
        date = response.meta['date']
      
        total_results = int(tree.find(f".//{{{prefix_dict['ns1']}}}totalResults").text)
        self.logger.info(f"parse_startttttt {total_results} url {url}")

        begin = 25
        
        if start_pos := url.find("&start=") != -1:
            begin = int(url[url.find("&start=") + 7 : len(url)])
            url = url[:url.find("&start=")]
            
        if total_results == 0:
            return
            
        if total_results > 8000:
            self.logger.info(f"more than 6000 in start")
    
        if total_results <= 5000:
            self.results += total_results
            self.logger.info(self.results)
            yield from self.parse(response)
            for start in range(begin, ((total_results - 1) // 25 + 1) * 25, 25):
                yield Request(f"{url}&start={start}",
                    headers = self.headers, callback = self.parse, dont_filter=True)
        else:
            self.logger.info(f"more than 5000")
        
        return


    def parse(self, response):
        root = fromstring(response.body)
        tree = ElementTree(root)
        
        articles = tree.findall(f".//{{{prefix_dict['ns0']}}}entry")
        
        self.articles_count += len(articles)
        self.logger.info(f"articles_count {self.articles_count}")
        
        for article in articles:
            article_item = APIArticleItem()           

            result = {}
            result['affiliations'] = []

            for affiliation in article.findall(f".//{{{prefix_dict['ns0']}}}affiliation"):
                affiliation_item = APIAffiliationItem()
                affiliation_item['name'] = getattr(affiliation.find(f"./{{{prefix_dict['ns0']}}}affilname"), 'text', "")
                affiliation_item['city'] = getattr(affiliation.find(f"./{{{prefix_dict['ns0']}}}affiliation-city"), 'text', "")
                affiliation_item['country'] = getattr(affiliation.find(f"./{{{prefix_dict['ns0']}}}affiliation-country"), 'text', "")

                result['affiliations'].append(affiliation_item)
                
            journal_item = APIJournalItem()
            journal_item['name'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}publicationName"), 'text', "")
            journal_item['issn'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}issn"), 'text', "")
            journal_item['e_issn'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}eIssn"), 'text', "")
            journal_item['aggregation_type'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}aggregationType"), 'text', "")
                      
            article_item['link_scopus'] = article.find(f"./{{{prefix_dict['ns0']}}}link[@ref='scopus']").attrib["href"]
            article_item['link_citedby'] = article.find(f"./{{{prefix_dict['ns0']}}}link[@ref='scopus-citedby']").attrib["href"]
            
            article_item['scopus_id'] = getattr(article.find(f"./{{{prefix_dict['dc']}}}identifier"), 'text', -1).replace("SCOPUS_ID:", "")
            article_item['eid'] = getattr(article.find(f"./{{{prefix_dict['ns0']}}}eid"), 'text', "")
            article_item['title'] = getattr(article.find(f"./{{{prefix_dict['dc']}}}title"), 'text', "")
      
            article_item['volume'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}volume"), 'text', -1)
            article_item['subject_id'] = subject_dict[self.subject]
            
            article_item['raw_xml'] = tostring(article, encoding='unicode')
            article_item['cur_time'] = datetime.datetime.now()
            article_item['api_key'] = api_keys[self.api_count]
        
            issue_identifier = article.find(f"./{{{prefix_dict['ns2']}}}issueIdentifier")
            if issue_identifier is None:
                article_item['issue_identifier'] = ""
            else:
                article_item['issue_identifier'] = issue_identifier.text
               
            citedby_count = article.find(f"./{{{prefix_dict['ns0']}}}citedby-count")
            if citedby_count is None:
                article_item['citedby_count'] = 0
            else:
                article_item['citedby_count'] = citedby_count.text 

            page_range = getattr(article.find(f"./{{{prefix_dict['ns2']}}}pageRange"), 'text', "")
            if page_range is None:
                article_item['page_range'] = ""
            else:
                article_item['page_range'] = page_range

            article_item['cover_date'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}coverDate"), 'text', "")
            article_item['doi'] = getattr(article.find(f"./{{{prefix_dict['ns2']}}}doi"), 'text', "")
            article_item['description'] = ""
            
            article_item['pubmed_id'] = getattr(article.find(f"./{{{prefix_dict['ns0']}}}pubmed-id"), 'text', "")
            article_item['subtype'] = getattr(article.find(f"./{{{prefix_dict['ns0']}}}subtype"), 'text', "")
            article_item['article_number'] = getattr(article.find(f"./{{{prefix_dict['ns0']}}}article-number"), 'text', "")
            article_item['source_id'] = getattr(article.find(f"./{{{prefix_dict['ns0']}}}source-id"), 'text', "")
            article_item['openaccess'] = getattr(article.find(f"./{{{prefix_dict['ns0']}}}openaccess"), 'text', "f")
            
            result['freetoread'] = []
            result['freetoread_label'] = []
                            
            if article_item['openaccess']:
                freetoread = article.find(f".//{{{prefix_dict['ns0']}}}freetoread")
                if(freetoread is not None):
                    for value in freetoread.findall(f".//{{{prefix_dict['ns0']}}}value"):
                        result['freetoread'].append(value.text)
                        
                freetoread_label = article.find(f".//{{{prefix_dict['ns0']}}}freetoreadLabel")   
                if(freetoread_label is not None):
                    for value in freetoread_label.findall(f".//{{{prefix_dict['ns0']}}}value"):
                        result['freetoread_label'].append(value.text)

            result['publication'] = article_item
            result['author'] = getattr(article.find(f"./{{{prefix_dict['dc']}}}creator"), 'text', "")
            result['journal'] = journal_item
            result['url'] = response.request.url
            yield result

            

