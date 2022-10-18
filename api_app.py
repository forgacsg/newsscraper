#%%
import requests
import pandas as pd
import datetime
import time
import os
import configparser
import psycopg2 


class Scraper():
    
    def __init__(self):
        self.df = pd.DataFrame()
        self.name = None


    def _colored(self, r, g, b, text):
        return f"\033[38;2;{r};{g};{b}m{text}\033[38;2;255;255;255m"
    
    def _connect_to_db(self):
        # to be implemented
        pass

    def _screen_clear(self):
    # for mac and linux(here, os.name is 'posix')
        if os.name == 'posix':
            _ = os.system('clear')
        else:
            # for windows platfrom
            _ = os.system('cls')
    
    def write_to_file(self):
        os.makedirs('./newsdata', exist_ok=True)
        self.df.to_csv(
            f'./newsdata/{self.name}_{str(datetime.date.today())}.csv', index=False)
        return 
    
    def write_to_db(self):
        def insert_data(connection, cursor):
            # if not self.df:
            #     print('no data present')
            #     return None
            table_name = self.table
            columns = ','.join(self.df.columns.tolist())
            for i in self.df.index:
                values = []
                for col in self.df.columns.tolist():
                    if type(self.df.at[i,col]) == str:
                        content = f"{self.df.at[i,col]}"
                        content = content.replace("'","")
                        content = f"\'{content}\'"
                        values += [content]
                    elif self.df.at[i,col] is None:
                        content = "\'NONE\'"
                        values += [content]
                    else:
                        values += [str(self.df.at[i,col])]
                [str(self.df.at[i,col]) for col in list(self.df.columns)]
                sql=f'INSERT INTO {table_name}({columns})\nVALUES ({",".join(values)});'
                
                # print(sql)
                cursor.execute(sql)
            connection.commit()
        
        conn = psycopg2.connect(
            host=self.host,
            database = self.db,
            user = self.user,
            password = self.password
        )
        cursor = conn.cursor()
        insert_data(connection = conn, cursor=cursor)




class NewsDataIOScraper(Scraper):

    def __init__(self, api_key, topics, host, db, user, passwd) -> None:
        super().__init__()
        self.api_key = api_key
        self.topics = ['%20'.join(topic.split()) for topic in topics]
        self.name = 'newsdataio'
        self.table = 'newsdataio'
        self.host = host
        self.db= db
        self.user=user
        self.password=passwd
        
    def send_request(self):

        
        for topic in self.topics:
            self._screen_clear()
            print('sending request to newsdata.io')
            
            print(f'starting topic: {self._colored(0,200,255,topic.replace("%20"," "))}')
            time.sleep(1)
            query_string = f'https://newsdata.io/api/1/news?apikey={self.api_key}&language=en&category=business,science,technology,top,world&q={topic}'
            print('sending request')
            
            r = requests.get(query_string)
            
            print(f'{self._colored(255,255,0,str(len(r.json()["results"])))} results found.')
            time.sleep(1)
            
            for result in r.json()['results']:
                keywords = ''
                if result['keywords']:
                    keywords = '|'.join(result['keywords'])

                data = pd.DataFrame({'topic': topic, 'title': result['title'], 'keywords': keywords, 'published': result['pubDate'],
                                    'description': result['description'], 'content': result['content'], 'link': result['link'], 'source': result['source_id']}, index=[0])
                self.df = self.df.append(data)

        self.df.reset_index(drop=True, inplace=True)
        
        # self.write_to_file()
        self.write_to_db()

class NewsApiOrgScraper(Scraper):
    
    def __init__(self, api_key, topics, domains, host, db, user,passwd):
        super().__init__()
        self.api_key = api_key
        self.topics =topics
        self.domains = domains
        self.name = 'newsapiorg'
        self.table = 'newsapiorg'
        self.host = host
        self.db= db
        self.user=user
        self.password=passwd
    
    def send_request(self):
        
        for topic in self.topics:
            # self._screen_clear()
            print(f'sending request to {self._colored(200,0,200,"NewsApi.org")}')
            print(f'starting topic: {topic}\n')

            for domain in self.domains:
                print(f'starting {self._colored(255,255,0, domain)}')

                # df = request_articles(topic=topic, apikey=apikey, domain=domain)
                startday = str(-datetime.timedelta(weeks=4))
                query_string = f"https://newsapi.org/v2/everything?q={topic}&from={startday}&sortBy=publishedAt&apiKey={self.api_key}&domains={domain}"
                r = requests.get(query_string)

                try:
                    articles = r.json()['articles']

                    for article in articles:
                        # print(article)
                        data = pd.DataFrame(article)
                        data = data[data.index == 'name']
                        data['topic'] = topic
                        self.df = self.df.append(data)
                    
                except:
                    print('no articles returned')  # requests are quickly outrun.
                    print(r.json())
                
                time.sleep(1)
        
        self.df.reset_index(drop=True, inplace=True)
        # self.write_to_file()
        # print(self.df.head(1))
        self.write_to_db()
        return


    
#%%   
if __name__ == "__main__":
    
    config = configparser.ConfigParser()
    config.read('config.ini')

    news_data = NewsDataIOScraper(
        api_key=config['NEWSDATA_IO']['api_key'], 
        topics=config['NEWSDATA_IO']['topics'].split(', '),
        host= config['DB_CONNECTION']['host'],
        db= config['DB_CONNECTION']['database'],
        user= config['DB_CONNECTION']['user'],
        passwd=config['DB_CONNECTION']['password']
        )
    news_data.send_request()
    
    news_api = NewsApiOrgScraper(
        api_key=config['NEWSAPI_ORG']['api_key'],
        topics=config['NEWSAPI_ORG']['topics'].split(', '),
        domains=config['NEWSAPI_ORG']['domains'].split(', '),
        host= config['DB_CONNECTION']['host'],
        db= config['DB_CONNECTION']['database'],
        user= config['DB_CONNECTION']['user'],
        passwd=config['DB_CONNECTION']['password']
        )
    news_api.send_request()
