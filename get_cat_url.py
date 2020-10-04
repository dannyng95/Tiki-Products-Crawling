from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd
import re

TIKI_URL = 'https://tiki.vn'
conn = sqlite3.connect('tiki.db')
cur = conn.cursor()


def get_url(url):
    try:
        response = requests.get(url).text
        soup = BeautifulSoup(response, 'html.parser')
        return soup
    except Exception as err:
        print('ERROR BY REQUEST:', err)



#Create cat table :
def create_categories_table():
    query = """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            url TEXT, 
            parent_id INTEGER,
            sub_categories INTEGER, 
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print('ERROR BY CREATE TABLE', err)
        
create_categories_table()

#create class category :

class Category:
    def __init__(self, name, url,sub_categories=None,parent_id=None, cat_id=None):
        self.cat_id = cat_id
        self.name = name
        self.url = url
        self.parent_id = parent_id
        self.sub_categories=sub_categories
        

    def __repr__(self):
        return f"ID: {self.cat_id}, Name: {self.name}, URL: {self.url}, Parent: {self.parent_id}, Sub-Category: {self.sub_categories}"

    def save_into_db(self):
        query = """
            INSERT INTO categories (name, url, parent_id)
            VALUES (?, ?, ?);
        """
        val = (self.name, self.url, self.parent_id)
        try:
            cur.execute(query, val)
            self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print('ERROR BY INSERT:', err)
    def update_sub_categories(self):
        query = """
            UPDATE categories
            SET sub_categories = ?
            WHERE id = ?;
        """
        val = (self.sub_categories, self.cat_id)
        try:
            cur.execute(query, val)
            conn.commit()
        except Exception as err:
            print('ERROR BY UPDATE:', err)


#GET MAIN CATEGORIES:            
def get_main_categories(save_db=True):
    soup = get_url(TIKI_URL)

    result = []
    for a in soup.find_all('a', {'class': 'MenuItem__MenuLink-sc-181aa19-1 fKvTQu'}):
        name = a.find('span', {'class': 'text'}).text
        url = a['href']
        main_cat = Category(name, url) # object from class Category

        if save_db:
            main_cat.save_into_db()
        result.append(main_cat)
    return result
#get sub categories and insert number of categories into table:

def get_sub_categories(parent_category, save_db=False):
    parent_url = parent_category.url
    parent_id = parent_category.cat_id
    result = []
    try:
        soup = get_url(parent_url)
        div_containers = soup.find_all('div', {'class':'list-group-item is-child'})
        parent_category.sub_categories = len(div_containers)
        parent_category.update_sub_categories()
        for div in div_containers:
            name = div.a.text
            # replace new name with " "
            name = re.sub('(\s{2,}|\n+)', ' ', name)

            sub_url = TIKI_URL + div.a['href']
            cat = Category(name, sub_url, parent_category.cat_id) # we now have parent_id, which is cat_id of parent category
            if save_db == True:
                cat.save_into_db()
            result.append(cat)
    except Exception as err:
        print('ERROR BY GET SUB CATEGORIES:', err)
    return result
   

# get_all_categories() given a list of main categories (This is a recursion function)
def get_all_categories(categories,save_db=False):
    if len(categories) == 0:
        return
    #else, go inside each category and update their sub again until len(categories) == 0
    for cat in categories:
        sub_categories = get_sub_categories(cat, save_db=save_db)
        print(f'{cat.name} has {len(sub_categories)} numbers of sub-categories')

        get_all_categories(sub_categories, save_db=save_db)

#RUN IT ON:
create_categories_table()
# cur.execute('DROP TABLE categories;')
# conn.commit()

main_categories = get_main_categories(save_db=True)

get_all_categories(main_categories,save_db=True)

df = pd.read_sql_query('''
select * from categories
''', conn)
df.to_excel('tiki_urls.xlsx', index=False)

#THIS SYNTAX FOR SORT THE LAST SUB-CAT 
Loop_url = pd.read_sql_query('''
    SELECT * FROM categories
    WHERE sub_categories = 0;
''', conn)
Loop_url.to_excel('tiki_no-sub-cat-urls.xlsx', index=False)

list_url =[i for i in Loop_url['url']]