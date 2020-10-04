import pandas as pd
loop_url = pd.read_excel('tiki_no-sub-cat-urls.xlsx')
lists_url =[i for i in loop_url['url']]
lists_url

#Done the 1st one, get list URL.
#Div 1 list for test:
lits_url_test= lists_url[100:120]
#Div into 10 list for crawl:
lists_url1 = lists_url[:250]
lists_url2 = lists_url[250:500]
lists_url3 = lists_url[500:750]
lists_url4 = lists_url[750:1000]
lists_url5 = lists_url[1000:1250]
lists_url6 = lists_url[1250:1500]
lists_url7 = lists_url[1500:1750]
lists_url8 = lists_url[1750:2000]
lists_url10 = lists_url[2000:]

#Create table in SQL:
import sqlite3
TIKI_URL = 'https://tiki.vn'
conn = sqlite3.connect('tiki.db')
cur = conn.cursor()


def create_products_table():
    query = """
        CREATE TABLE IF NOT EXISTS products (
            Product_id INTEGER,
            Product_sku INTEGER,
            Seller_id INTEGER,
            Product_Title VARCHAR(255),
            Product_Category VARCHAR(255),
            Product_Brand VARCHAR(255),
            Author VARCHAR(255),
            url TEXT, 
            img_URL TEXT,
            Regular_price INTEGER,
            Final_price INTEGER,
            Discount INTEGER,
            Comment TEXT,
            TIKI_NOW TEXT,
            Rating TEXT,
            Installment TEXT,
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)
        
create_products_table()

#define CLass product:
class Products:
    def __init__(self, Product_id, Product_sku, Seller_id, Product_Category, Product_Title, Product_Brand, Author, url, img_URL, Regular_price, Final_price, Discount, Comment, TIKI_NOW, Rating, Installment):
        self.Product_id = Product_id
        self.Product_sku = Product_sku
        self.Seller_id = Seller_id
        self.Product_Category = Product_Category
        self.Product_Title = Product_Title
        self.Product_Brand= Product_Brand
        self.Author = Author
        self.url  = url 
        self.img_URL = img_URL
        self.Regular_price  = Regular_price 
        self.Final_price= Final_price
        self.Discount = Discount
        self.Comment = Comment 
        self.TIKI_NOW = TIKI_NOW
        self.Rating = Rating
        self.Installment=Installment
        
    def __repr__(self):
        return f"Product ID: {self.Product_id}, PRODUCT SKU: {self.Product_sku}, SELLER ID: {self.Seller_id}, PRODUCT CAT: {self.Product_Category}, PRODUCT TITLE: {self.Product_Title}, PROUCT BRAND: {self.Product_Brand}, AUTHOR: {self.Author}, URL: {self.url}, IMG URL: {self.img_URL}, REGULAR PRICE: {self.Regular_price}, FINAL PRICE: {self.Final_price}, DISCOUNT: {self.Discount}, COMMENT: {self.Comment}, TIKI NOW: {self.TIKI_NOW}, Rating: {self.Rating}, Installment:{self.Installment}  "

    def save_into_db(self):
        query = """
            INSERT INTO products (Product_id, Product_sku, Seller_id,Product_Category, Product_Title, Product_Brand, Author, url, img_URL, Regular_price, Final_price, Discount, Comment, TIKI_NOW, Rating, Installment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        val = (self.Product_id, self.Product_sku, self.Seller_id,self.Product_Category, self.Product_Title, self.Product_Brand, self.Author, self.url, self.img_URL, self.Regular_price, self.Final_price, self.Discount, self.Comment, self.TIKI_NOW, self.Rating, self.Installment)
        try:
            cur.execute(query, val)
            self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)


#function for getting url and soup :

import pickle
import pandas as pd
import requests
import random
import time
from time import sleep
from bs4 import BeautifulSoup
from random import randint

def get_url(url):
    try:
        response = requests.get(url).text
        soup = BeautifulSoup(response, 'html.parser')
        return soup # beautiful object 
    except Exception as err:
        print('ERROR BY REQUEST:', err)

def save_to_pickle(fname,obj):
    with open(f'{fname}.pickle', 'wb') as f:
        pickle.dump(obj, f)

def Get_HTML(lists_url): 
  # get all products from all lowest category URLs (lists_url)
  # Output: list of all beautifulsoup products from all the lowest cat URLs

  all_products=[]
  #when dealing with iterators, we also get a need to keep a count of iteration => using enumerate
  for i,url in enumerate(lists_url):
      print(f'Scraping index {i} which is {url}') # print out which is categories is on.

      pages = [str(i) for i in range(1,2)] # scrawl 1 page for each category. Recommend: 2 pages per cat
      for page in pages:
        url_base = f"{url}&page={page}"
        print(f'  Scraping {url_base}') #  print out which page inside a categories
        #return url_base
        soup = get_url(url_base)

        products = soup.find_all('div',{'class':'product-item'}) # list of beautifulsoup obj, contain each product
        if (len(products)==0):
          break
        # products = crawling_product(products)
        all_products+= products

        # sleep!
        
        # save file after n products
        # if len(all_products) == 1000: # customized
        #   save_to_pickle(all_products)
        #   all_products=[]
          
  return all_products


  def crawling_product(product_html,save_db):
  # Crawl 1 single product
  # Input: beautifulsoup object of 1 product
  # Output: object of class Product
    product = None
    try:
      Product_id = product_html.a['data-id']
      Product_sku = product_html['product-sku']
      Seller_id = product_html['data-seller-product-id']
      Product_Category = product_html['data-category']
      Product_Title = product_html.a['title']
      Product_Brand = product_html['data-brand']
      url = (f"https://tiki.vn{product_html.a['href']}")
      img_URL = product_html.a.div.span.img['src']

      Final_price = product_html.find('span',{'class':'final-price'})
      Final_price = Final_price.text.strip(" \n, ...")
      
      #Let's Check for somethings deeper
      #COMMENT
      try: 
        Comment = product_html.find('p',{'class':'review'})
        Comment = Comment.text.strip(" \n, ')' (... ")
      except:
        Comment = None
      try:
        Discount = product_html.find('span',{'class':'sale-tag sale-tag-square'})
        Discount = Discount.text
      except:
        Discount = None
      #AUTHOR for BOOKs
      try:
        Author = product_html.find('p',{'class':'author'})
        Author = Author.text
      except:
        Author = None
      #REGULAR PRICE (sometime product has the same with final price)
      try:
        #Some products doesn't have regular price
        Regular_price = product_html.find('span',{'class':'price-regular'})
        Regular_price = Regularprice.text.strip(" \n, ...")
      except:
        #print(f"No Regular Price for {d['Product-Title']}")    
        Regular_price = Final_price
      #INSTALLMENT - For optional
      try:                          
        #Or somethings like Installment
        Installment = product_html.find('span',{'class':'installment-price-v2'})
        Installment = Installment.text
      except:
        #print(f"No installment for {d['Product-Title']}")
        Installment = None                

        #RATING is somes of optionals
      try:
        Rating = product_html.find('span',{'class':'rating-content'})
        Rating = str(Rating.span['style']).strip("width:")
      except:
        #print(f"No Rating for {d['Product-Title']}")
        Rating = None                       
        #TIKI-NOW - aka Unique Value of TIKI
      try:                   
        tikinow = product_html.find('div',{'class':'badge-service'})
        badge_check = tikinow.div.img['src']
        if str(badge_check) == "https://salt.tikicdn.com/ts/upload/9f/32/dd/8a8d39d4453399569dfb3e80fe01de75.png":
          TIKI_NOW = "YES"
        TIKI_NOW = None
      except:
        TIKI_NOW = None 
      
      
      product = Products(Product_id, Product_sku, Seller_id, Product_Category, Product_Title, Product_Brand, Author, url, img_URL, Regular_price, Final_price, Discount, Comment, TIKI_NOW, Rating, Installment)
      
      
      if save_db==True:
        product.save_into_db()
    except Exception as err:
        print('ERROR BY GET PRODUCT FROM PRODUCTS:', err)
    return product


# GO BOTH HERE :

def crawling_all_products(products_b4,save_db=False):
  # Input: list of beautifulsoup object of all products
  # Output: list of PRODUCT object (of all products)
  product_objects=[]
  for i,product in enumerate(products_b4):
    print(f'Scraping product at index {i}')
    crawled_prod = crawling_product(product,save_db) # crawl 1 product
    product_objects.append(crawled_prod)

    # if i%20==0:  # crawl 20 products, sleep 
        # sleep

  return product_objects



  #
all_products1 = Get_HTML(lists_url1) # list of BEAUTIFULSOUP object
# save all_products1 to pickle
all_products2 = Get_HTML(lists_url2)
all_products3 = Get_HTML(lists_url3)
all_products4 = Get_HTML(lists_url4)
all_products5 = Get_HTML(lists_url5)
all_products6 = Get_HTML(lists_url6)
all_products7 = Get_HTML(lists_url7)
all_products8 = Get_HTML(lists_url8)
all_products9 = Get_HTML(lists_url9)
all_products10 = Get_HTML(lists_url10)

#rRUNNING: 
all_products_testing = Get_HTML(all_products1)

product_obj_testing= crawling_all_products(all_products1,save_db=True)


pd.read_sql('SELECT * FROM products;',conn)