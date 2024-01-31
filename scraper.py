import requests
from bs4 import BeautifulSoup
import pymongo
from clickhouse_driver import Client
import json
 
# Запрос данных со страницы
def scrape_books():
    url = 'http://books.toscrape.com/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    books = soup.find_all('article', class_='product_pod')
    book_data = []
    
    # Извлечение информации о книгах
    for book in books:
        title = book.h3.a['title']
        price = float(book.find('p', class_='price_color').text[1:])
        stock = book.find('p', class_='instock availability').text.strip()
        stock = int(stock.split()[2])
        description = book.find('p', class_='').text
        book_info = {
            'title': title,
            'price': price,
            'stock': stock,
            'description': description
        }
        book_data.append(book_info)
    
    return book_data
 
# Сохранение данных в JSON-файл
def save_to_json(data):
    with open('books.json', 'w') as f:
        json.dump(data, f)
 
# Сохранение данных в MongoDB
def save_to_mongodb(data):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['bookstore']
    collection = db['books']
    collection.insert_many(data)
 
# Сохранение данных в ClickHouse
def save_to_clickhouse(data):
    client = Client('localhost')
    client.execute('CREATE DATABASE IF NOT EXISTS bookstore')
    client.execute('CREATE TABLE IF NOT EXISTS bookstore.books (title String, price Float64, stock Int32, description String) ENGINE = Memory')
    client.execute('INSERT INTO bookstore.books VALUES', data)
 
# Запуск скрапинга и сохранение данных
if __name__ == '__main__':
    book_data = scrape_books()
    save_to_json(book_data)
    save_to_mongodb(book_data)
    save_to_clickhouse(book_data)