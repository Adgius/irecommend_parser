import undetected_chromedriver as uc
import re
import pandas as pd
import time
import random
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

class EboboParser():

    def __init__(self):
        os.makedirs('output', exist_ok=True)
        self.head_link = 'https://irecommend.ru'
        options  = uc.ChromeOptions()
        options.add_argument("--headless")
        #options.capabilities['pageLoadStrategy'] = "none"
        #self.driver = uc.Chrome(options = options)
        self.driver = uc.Chrome()

    def random_sleep(self):
        time.sleep(random.randint(1, 3))

    def notice_product(self, prod_link):
        products_table = pd.read_csv(r'output\products_link.csv', sep=';', index_col=0)
        products_table.loc[products_table.index == prod_link, 'parsed'] = True
        products_table.to_csv(r'output\products_link.csv', sep=';')
    
    def parse_products(self, url, page):
        products_table = pd.DataFrame(columns=['title', 'rating', 'parsed'])
        self.driver.get(url + f'?page={page}')
        products_page = self.driver.page_source
        soup = BeautifulSoup(products_page, 'html.parser')
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

    def get_reviews(self, prod_url, check_pages=True):
        reviews_link = pd.DataFrame(columns=['prod_link', 'review_link', 'review'])
        self.driver.get(prod_url)
        reviews_page = self.driver.page_source
        soup = BeautifulSoup(reviews_page, 'html.parser')

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


    def parse_review_text(self, url):
        self.driver.get(url)
        self.random_sleep()
        review_page = self.driver.page_source
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

    def agg_reviews_text(self, prod_link, review_pages,  reviews_link):
        
        print(f'|------Нашлось {review_pages} страниц отзывов')

        def save_review(reviews_link):
            for review_link in tqdm(reviews_link):
                title, text = self.parse_review_text(review_link)
                table = pd.read_csv(r'output\reviews_link.csv', sep=';')
                key = table['prod_link'] == prod_link
                key = key & (table['review_link'] == review_link)
                table.loc[key, 'review'] = [{'title': title, 'text': text}]
                table.to_csv(r'output\reviews_link.csv', index=False, sep=';')

        random.shuffle(reviews_link)
        save_review(reviews_link)
        if review_pages > 1:
            for review_page in range(1, review_pages):
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
    parser.main('https://irecommend.ru/catalog/list/31')