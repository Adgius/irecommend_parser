import undetected_chromedriver as uc
import re
import pandas as pd
import time
import random
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

import socket
import socks
from urllib.request import urlopen
from stem import Signal
from stem.control import Controller

from fake_useragent import UserAgent
import requests
from requests.exceptions import ConnectionError, ReadTimeout


class EboboParser():

    def __init__(self):
        os.makedirs('output', exist_ok=True)
        self.head_link = 'https://irecommend.ru'
        self.ua = UserAgent(use_cache_server=False)
        self.controller = Controller.from_port(port=9051)
        self.n_req = 0

    def change_IP(self):
        socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9050, True)
        socket.socket = socks.socksocket
        self.controller.authenticate("228")
        self.controller.signal(Signal.NEWNYM)
        print('New IP:', urlopen('http://icanhazip.com').read())

    def random_sleep(self):
        time.sleep(random.randint(3, 6))

    def notice_product(self, prod_link):
        products_table = pd.read_csv(r'output\products_link.csv', sep=';', index_col=0)
        products_table.loc[products_table.index == prod_link, 'parsed'] = True
        products_table.to_csv(r'output\products_link.csv', sep=';')
    
    def parse_products(self, url, page):
        try:
            products_table = pd.DataFrame(columns=['title', 'rating', 'parsed'])
            r = requests.get(url + f'?page={page}', headers={'User-Agent': self.ua.chrome}, timeout=15)
            self.random_sleep()
            products_page = r.content
            soup = BeautifulSoup(products_page, 'html.parser')

            self.n_req += 1
            if self.n_req % 5 == 0:
                self.change_IP()

            products = soup.select('.ProductTizer.plate.teaser-item')
            print(f'|-Опа ча. На {page+1} странице нашел {len(products)} продуктов')
            for p in products:
                title = p.find('div', {'class': 'title'}).text
                rating = p.find('span', {'class': 'average-rating'}).text
                rating = float(re.search(r'[\d\.]+', rating)[0])
                link = self.head_link + p.find('a', {'class': 'read-all-reviews-link'})['href']
                products_table.loc[link] = [title, rating, False]   
            if os.path.isfile(r'output\products_link.csv'):
                prod = pd.read_csv(r'output\products_link.csv', sep=';', index_col=0)
                products_table = pd.concat([prod, products_table])
                products_table = products_table.sort_values('parsed', ascending=False).drop_duplicates(['title', 'rating'], ignore_index=False)
            products_table['parsed'] = products_table['parsed'].astype(bool)
            products_table.to_csv(r'output\products_link.csv', sep=';')
            return products_table[~products_table['parsed']].index.values
        except (ConnectionError, AttributeError, ReadTimeout) as e:
            print(e)
            self.change_IP()
            return []

    def get_reviews(self, prod_url, check_pages=True):
        try:
            reviews_link = pd.DataFrame(columns=['prod_link', 'review_link', 'review'])
            r = requests.get(prod_url, headers={'User-Agent': self.ua.chrome}, timeout=15)
            self.random_sleep()

            self.n_req += 1
            if self.n_req % 5 == 0:
                self.change_IP()

            reviews_page = r.content
            soup = BeautifulSoup(reviews_page, 'html.parser')
            prod_url = prod_url.split('?')[0] #Убираем номер страницы (чтобы в табличке ссылки не дубл)
            title = soup.find('h1', {'class': 'largeHeader'}).find('span').text
            print(f'|---Парсим отзывы {title}')
            items = soup.find('ul', {'class': 'list-comments'})
            reviews = items.find_all('li')
            random.shuffle(reviews)
            review_pages = 1
            for n, rev in enumerate(reviews):
                review_link = self.head_link + rev.select('div.reviewTextSnippet a.more')[0]['href']
                reviews_link.loc[n, ] = [prod_url, review_link, None]
            if check_pages:
                if soup.find('ul', {'class': 'pager'}):
                    review_pages = int(soup.find('li', {'class': 'pager-last'}).text)
            if os.path.isfile(r'output\reviews_link.csv'):
                table = pd.read_csv(r'output\reviews_link.csv', sep=';')
                reviews_link = pd.concat([table, reviews_link])
                reviews_link = reviews_link.sort_values('review').drop_duplicates(['prod_link', 'review_link'], ignore_index=False)
            reviews_link.to_csv(r'output\reviews_link.csv', index=False, sep=';')
            self.random_sleep()
            return review_pages, reviews_link.loc[reviews_link['review'].isna(), 'review_link'].values
        except (ConnectionError, AttributeError, ReadTimeout) as e:
            print(e)
            self.change_IP()
            return 0, []


    def parse_review_text(self, url):
        try:
            r = requests.get(url, headers={'User-Agent': self.ua.chrome}, timeout=15)
            self.random_sleep()

            self.n_req += 1
            if self.n_req % 5 == 0:
                self.change_IP()

            review_page = r.content
            soup = BeautifulSoup(review_page, 'html.parser')
            title = soup.find('h2', {'class': 'reviewTitle'}).text
            part = ''
            for sel in soup.select('.description.hasinlineimage ul, p'):
                first = True
                for t in sel:
                    if re.search(r'\s{3,}', t.text):
                        break
                    if sel.find('li'):
                        if first:
                            part += t.text
                            first = False
                        else:
                            part += ', ' + t.text
                    else:
                        first = False
                        part += t.text + '\n'
            return title, part
        except (ConnectionError, AttributeError, ReadTimeout) as e:
            print(e)
            self.change_IP()
            return None

    def agg_reviews_text(self, prod_link, review_pages,  reviews_link):
        
        print(f'|------Нашлось {review_pages} страниц отзывов')

        def save_review(reviews_link):

            if self.n_req % 5 == 0:
                self.change_IP()

            for review_link in tqdm(reviews_link):
                output = self.parse_review_text(review_link)
                table = pd.read_csv(r'output\reviews_link.csv', sep=';')
                #key = table['prod_link'] == prod_link
                #key = key & (table['review_link'] == review_link)
                key = table['review_link'] == review_link
                if output:
                    title, text = output
                    table.loc[key, 'review'] = [{'title': title, 'text': text}]
                else:
                    table.loc[key, 'review'] = None
                table.to_csv(r'output\reviews_link.csv', index=False, sep=';')

        random.shuffle(reviews_link)
        save_review(reviews_link)
        if review_pages > 1:
            for review_page in range(2, review_pages+1):
                _,  reviews_link = self.get_reviews(prod_link + f'?page={review_page}', check_pages=False)
                self.random_sleep()
                random.shuffle(reviews_link)
                save_review(reviews_link)
            
            
                        
    def main(self, url):

        """
        url: str -- ссылка на раздел продуктов

        1. Функция открывает браузер и переходит по ссылке (цикл 1 -- страницы)
            2. Получает список продуктов на странице
            3. Рандомно пробегается по продуктам (цикл 2 -- продукты)
            4. Внутри каждого продукта получает ссылки на отзывы 
                5. Если несколько страниц отзывов (цикл 3 -- страницы)
                    5. Рандомно парсит отзывы
        """

        self.change_IP()
 
        for page in range(100):
            prod_links = self.parse_products(url, page)
            self.random_sleep()
            random.shuffle(prod_links)
            for prod_link in prod_links:
                review_pages,  reviews_link = self.get_reviews(prod_link)
                self.random_sleep()
                self.agg_reviews_text(prod_link, review_pages,  reviews_link)
                self.notice_product(prod_link)


    

if __name__ == '__main__':
    print("""
        ######   #####       ###     #####       ###
        ##       ##   #    ##   ##   ##   #    ##   ##
        ######   #####    ##     ##  #####    ##     ##
        ##       ##   #    ##   ##   ##   #    ##   ##
        ######   #####       ###     #####       ###

    """)
    parser = EboboParser()
    parser.main('https://irecommend.ru/catalog/list/43941-44139')