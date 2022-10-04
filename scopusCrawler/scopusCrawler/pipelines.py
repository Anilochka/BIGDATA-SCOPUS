# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

# pipelines.py
import logging
import psycopg2

from itemadapter import ItemAdapter

class ScopusscrawlerPipeline:
    def __init__(self):
        ## Connection Details
        hostname = 'localhost'
        username = 'denis'
        password = 'qw' # your password
        database = 'denis'

        ## Create/Connect to database
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.connection.set_client_encoding('UTF8')
        ## Create self.cur, used to execute commands
        self.cur = self.connection.cursor()
        
        self.freetoread = {
            'all': 1,
            'publisherhybridgold': 2,
            'repository': 3,
            'repositoryvor': 4,
            'repositoryam': 5,
            'publisherfree2read': 6
        }
        
        self.freetoread_label = {
            'All Open Access': 1,
            'Green': 2,
            'Hybrid Gold': 3
        }
        #
        
    def process_item(self, item, spider):
       # try:
        spider.log('Working Element %s.' % item['publication']['scopus_id'])
        
        self.cur.execute("""WITH cte AS (
                       INSERT INTO journal(scopus_id, name, issn, e_issn, aggregation_type, cite_score, sjr, snip, year_of_scores)
                       values (-1,%(name)s,%(issn)s,%(e_issn)s,%(aggregation_type)s, 0, 0, 0, '')
                       ON CONFLICT (issn, e_issn) DO NOTHING
                       RETURNING id
                    )
                    SELECT id from cte
                    UNION ALL
                    SELECT id  
                    FROM journal
                    WHERE issn = %(issn)s and e_issn = %(e_issn)s AND not exists (select 1 from cte);""" , item["journal"])
            
        journal_id = self.cur.fetchone()
        item["publication"]['journal_id'] = journal_id
        
        self.cur.execute("""WITH cte AS (
                       INSERT into publication (link_scopus, link_citedby, dc_identifier, eid, title, volume, issue_identifier, page_range,
                       cover_date, doi, citedby_count, pubmed_id, journal_id, subtype, article_number, source_id, openaccess, authkeywords, description, raw_xml, cur_time, api_key)
                       values (%(link_scopus)s,%(link_citedby)s, %(scopus_id)s,%(eid)s,%(title)s, %(volume)s,%(issue_identifier)s,%(page_range)s,
                       %(cover_date)s,%(doi)s,%(citedby_count)s, %(pubmed_id)s,%(journal_id)s,%(subtype)s, %(article_number)s,%(source_id)s,%(openaccess)s, '', '',%(raw_xml)s, %(cur_time)s, %(api_key)s) 
                       ON CONFLICT (eid) DO NOTHING RETURNING id
                    )
                    SELECT true, id, -1 from cte
                    UNION
                    SELECT false, id, dc_identifier
                    FROM publication
                    WHERE dc_identifier = %(scopus_id)s AND not exists (select 1 from cte);""", item["publication"])
        
        publication_id = self.cur.fetchone()
        self.cur.execute("""insert into publication_subject (publication_id, subject_id) values
            (%(publication_id)s, %(subject_id)s) ON CONFLICT (publication_id, subject_id) DO NOTHING;""", {'publication_id': publication_id[1], 'subject_id': item['publication']['subject_id']})
        
        #spider.logger.info(f'publication_id {publication_id} url {item["url"]}')
        #spider.logger.info(f'publication {item["publication"]}')
        
        if publication_id[0]:
            afid_ids = []
            if item["author"] != "":
                self.cur.execute("""insert into author (auid, name) values (-1,%(author)s) returning id;""", {'author': item["author"]})
                author_id = self.cur.fetchone()

                for affiliation in item["affiliations"]:
                    self.cur.execute("""WITH cte AS (
                        INSERT INTO affiliation(afid, name, city, country)
                        values (-1, %(name)s, %(city)s, %(country)s)
                        ON CONFLICT (name, city, country) DO NOTHING
                        RETURNING id
                    )
                    SELECT id from cte
                    UNION ALL
                    SELECT id  
                    FROM affiliation
                    WHERE name = %(name)s and city = %(city)s and country = %(country)s AND not exists (select 1 from cte);""", affiliation)
     
                    afid_ids += self.cur.fetchone()
            
            for afid_id in afid_ids:
                self.cur.execute("""insert into publication_author (publication_id, author_id, affiliation_id) values
                    (%(publication_id)s, %(author_id)s, %(affiliation_id)s) ON CONFLICT (publication_id, author_id, affiliation_id) DO NOTHING;""", {'publication_id': publication_id[1], 'author_id': author_id, 'affiliation_id': afid_id})
                    
            if item["publication"]["openaccess"]:
                for freetoread in item["freetoread"]:
                    if freetoread not in self.freetoread:
                        self.cur.execute("""insert into freetoread (name) values
                            (%(name)s) returning id;""", {'name': freetoread})
                        self.freetoread[freetoread] = self.cur.fetchone()
                    
                    self.cur.execute("""insert into publication_freetoread (publication_id, freetoread_id) values
                    (%(publication_id)s, %(freetoread_id)s) ON CONFLICT (publication_id, freetoread_id) DO NOTHING;""", {'publication_id': publication_id[1], 'freetoread_id': self.freetoread[freetoread]})
                    
                    
                for freetoread_label in item["freetoread_label"]:
                    if freetoread_label not in self.freetoread_label:
                        self.cur.execute("""insert into freetoread_label (name) values
                            (%(name)s) returning id;""", {'name': freetoread_label})
                        self.freetoread_label[freetoread_label] = self.cur.fetchone()
                    
                    self.cur.execute("""insert into publication_freetoread_label (publication_id, freetoread_label_id) values
                    (%(publication_id)s, %(freetoread_label_id)s) ON CONFLICT (publication_id, freetoread_label_id) DO NOTHING;""", {'publication_id': publication_id[1], 'freetoread_label_id': self.freetoread_label[freetoread_label]})

        elif publication_id[2] == "":
            #spider.logger.info(f'pub updated eid {item["publication"]["eid"]}')
            self.cur.execute("""UPDATE publication SET link_scopus =%(link_scopus)s, link_citedby =%(link_citedby)s, dc_identifier = %(scopus_id)s, 
                       eid = %(eid)s, title = %(title)s, volume = %(volume)s, issue_identifier = %(issue_identifier)s, page_range = %(page_range)s,
                       cover_date = %(cover_date)s, doi = %(doi)s, citedby_count = %(citedby_count)s, pubmed_id = %(pubmed_id)s, journal_id = %(journal_id)s, 
                       subtype = %(subtype)s, article_number = %(article_number)s, source_id = %(source_id)s, openaccess = %(openaccess)s, authkeywords = '', description = '',
                       raw_xml = %(raw_xml)s, cur_time = %(cur_time)s, api_key = %(api_key)s WHERE eid = %(eid)s;""", item["publication"])
            
            
        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()
