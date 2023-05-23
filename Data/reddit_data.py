import time
import os
import pandas as pd
import json
import requests
import lxml.html as lh
import pickle
import random
import threading
from datetime import datetime
from retrying import retry
from selenium_helper import helper
from selenium.webdriver.common.by import By

helper = helper(True)
driver = helper.driver

class DFUtils:
    
    def __init__(self) -> None:
        self.pickle_path = 'urls.pickle'
        self.df_path = 'data.csv'
        self.df_cols = ['post_time', 'title', 'comments', 'nsfw', 'comments_url']
        self.time = lambda: datetime.now().strftime("%H:%M:%S")
        
    def get_pickle(self) -> list:
        if not os.path.exists(self.pickle_path):
            return []
        else:
            with open(self.pickle_path, 'rb') as f:
                return pickle.load(f)
            
    def get_pickle_len(self) -> int:
        return len(self.get_pickle())
        
    def get_df(self) -> pd.DataFrame:
        if not os.path.exists(self.df_path):
            return pd.DataFrame(columns=self.df_cols)
        else:
            return pd.read_csv('data.csv')
        
    def get_df_items(self, column) -> list:
        return self.get_df()[column].tolist()
        
    def get_df_len(self) -> int:
        return len(self.get_df())
    
    def update_pickle_data(self, post_data: list) -> None:
        if not post_data:
            return
        
        pickle_data = self.get_pickle()
        pickle_data.extend(post_data)
        
        with open(self.pickle_path, 'wb') as f:
            pickle.dump(pickle_data, f)
        
        print(f'{self.time()} Writing {len(post_data)} posts to {self.pickle_path}.pickle with {self.get_pickle_len()} total posts')
        
    def update_df_data(self, comment_data: pd.DataFrame) -> None:
        if comment_data.empty:
            return
        
        df = self.get_df()
        df = pd.concat([df, comment_data])
        df.to_csv('data.csv', index=False)
        
        print(f'{self.time()} Writing {len(comment_data)} comments to {self.df_path}.csv with {self.get_df_len()} total posts')

class Scraper(DFUtils):
    
    def __init__(self, subreddit: str, limit=100, buffer: str or int = '1 hour', comment_buffer: int = 1, persist_index: bool = False) -> None:
        super().__init__()

        self.subreddit = subreddit
        self.persist_index = persist_index
        self.err = 0
        self.limit = limit
        self.buffer = Scraper.get_time_ago(buffer)
        self.comment_buffer = comment_buffer
        self.cur_div = int(open('cur_div.txt', 'r').read()) if os.path.exists('cur_div.txt') and persist_index else 2
        self.urls = [f'https://www.reddit.com/r/{subreddit}/new/', 'https://www.reddit.com/r/{subreddit}/rising/', 'https://www.reddit.com/r/{subreddit}/controversial/',
                     f'https://www.reddit.com/r/{subreddit}/', f'https://www.reddit.com/r/{subreddit}/top/?t=hour', f'https://www.reddit.com/r/{subreddit}/top/?t=day', 
                     f'https://www.reddit.com/r/{subreddit}/top/?t=week', f'https://www.reddit.com/r/{subreddit}/top/?t=month', f'https://www.reddit.com/r/{subreddit}/top/?t=year', 
                     f'https://www.reddit.com/r/{subreddit}/top/?t=all']
        self.s = requests.Session()
        
    def get_posts(self) -> list:
        post_data = []

        while True:
            try:
                post = helper.locate(f'//*[@id="AppRouter-main-content"]/div/div/div[2]/div[4]/div[1]/div[5]/div[{self.cur_div}]', wait_time=0.1, exception=False)
                
                if post == None:
                    self.err += 1
                    break
                else:
                    self.cur_div += 1
                    self.err = 0
                    if self.persist_index:
                        open('cur_div.txt', 'w').write(str(self.cur_div))
                try:
                    promoted = post.find_element(By.XPATH, './/div/div/div/div[3]/div/div[1]/div/div[1]/span[2]/span')
                except:
                    promoted = False
                if promoted != False:
                    continue
                post_time = post.find_element(By.XPATH, './/div/div/div[3]/div[1]/div/div[1]/span[2]').text
                post_time = Scraper.get_time_ago(post_time)
                # if post_time > self.buffer:
                #     continue
                
                post_title = post.find_element(By.XPATH, './/div/div/div[3]/div[2]/div[1]/a/div/h3').text
                comments_url = post.find_element(By.XPATH, './/div/div/div[3]/div[2]/div[1]/a').get_attribute('href')
                comments_url = comments_url.split('comments/')[-1] # https://www.reddit.com/r/AskReddit/comments/13ol3cg/whats_something_that_you_wish_you_could_enjoy/
                
                cur_items = self.get_df_items('comments_url')
                if comments_url in cur_items:
                    continue
                
                post_data.append((post_title, comments_url, None, None))
            except:
                continue
        return post_data
            
    def get_comments(self, link, max_comments=10) -> pd.DataFrame:
        
        initial_headers = {
            'Authority': 'www.reddit.com',
            'Method': 'GET',
            'Path': '/r/CBSE/comments/13pa2xh/my_friends_mom_forced_him_to_take_pcmb/',
            'Scheme': 'https',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Chromium";v="96", "Google Chrome";v="96", ";Not A Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }
        
        id = link.split('/')[0]

        page_initial = self.s.get(f'https://www.reddit.com/r/{self.subreddit}/comments/{link}', headers=initial_headers)
        page_content = self.s.get(f'https://www.reddit.com/svc/shreddit/comments/{self.subreddit.lower()}/t3_{id}?render-mode=partial&shredtop=')
        doc_initial = lh.fromstring(page_initial.content)
        doc = lh.fromstring(page_content.content)

        comments = doc.xpath('//shreddit-comment/div/div[@id="-post-rtjson-content"]/p/text()')
        comments = comments[:max_comments] if len(comments) > max_comments else comments
        comments = '-----'.join(comments)
        
        title = doc_initial.xpath('//div[contains(@id, "post-title")]/text()')[0]
        description = doc_initial.xpath(f'//div[contains(@id, "{id}")]/p/text()')
        description = description[0] if len(description) > 0 else ''
        title = '---' + title + description
        
        post_time = doc_initial.xpath(f'//faceplate-timeago[contains(@class, "whitespace-nowrap text-neutral-content-weak")]/@ts')[0].split('000+')[0] + 'Z'
        post_time = int(datetime.strptime(post_time, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp())

        nsfw = json.loads(doc_initial.xpath('//shreddit-screenview-data/@data')[0])["post"]["nsfw"]
        
        df = pd.DataFrame([(post_time, title, comments, nsfw, link)], columns=self.df_cols)
        
        return df

    def post_main(self) -> None:
        
        while self.get_pickle_len() < self.limit:
            posts = self.get_posts()
            self.update_pickle_data(posts)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            if self.err > 50:
                driver.get(random.choice(self.urls))
                self.err = 0
                self.cur_div = 2
                print(f'{self.time()} Switching to {self.urls[self.url_index]}')

            time.sleep(1)
            
    def comment_main(self) -> None:
        for link in self.get_pickle():
            df = self.get_comments(link)
            self.update_df_data(df)

            # time.sleep(0.5)
            
    @retry(wait_fixed=1000)  # Retry every 1 second
    def retry_on_error(self, func):
        return func()
            
    def main(self) -> None:
        post_thread = threading.Thread(target=self.retry_on_error, args=(self.post_main,))
        post_thread.daemon = True  # Allow the program to exit if only daemon threads are remaining

        comment_thread = threading.Thread(target=self.retry_on_error, args=(self.comment_main,))
        comment_thread.daemon = True

        post_thread.start()
        comment_thread.start()

        post_thread.join()
        comment_thread.join()

    def get_time_ago(time_str: str) -> str:
        
        intervals = {
            'second' : 1, 'seconds': 1,
            'minute': 60, 'minutes': 60,
            'hour': 3600, 'hours': 3600,
            'day': 86400, 'days': 86400,
            'week': 604800, 'weeks': 604800,
            'month': 2629800, 'months': 2629800,
            'year': 31557600, 'years': 31557600,
        }

        current_time = int(time.time())
        time_difference = 0
        
        if time_str == 'just now':
            return str(current_time)

        for word in time_str.split():
            if word.isdigit():
                time_difference += int(word)
            else:
                word = word.lower()
                if word in intervals:
                    time_difference += intervals[word]

        return str(current_time - time_difference)
    
if __name__ == '__main__':
    scraper = Scraper('AskReddit', limit=20000)
    scraper.main()