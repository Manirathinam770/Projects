import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from time import sleep
import pandas as pd
import datetime
import json 
from json import dumps
from kafka import KafkaProducer


def scraped_data_india(url):
    # ... (your existing code)
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')  # Enable headless mode

    # Initialize the WebDriver with the specified options
    driver = webdriver.Firefox(options=options)

    # Define the URL to scrape
    link = f'https://finance.yahoo.com/quote/{url}'
    driver.get(link)

    name = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[2]/div[1]/div[1]/h1').text
    Indian_market_price_str = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div/fin-streamer[1]').text
    change = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div/fin-streamer[2]/span').text
    change_percnt = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div/fin-streamer[3]/span').text
    
    # Check if Indian_market_price_str contains commas and remove them
    if ',' in Indian_market_price_str:
        Indian_market_price_str = Indian_market_price_str.replace(',', '')
    
    # Convert strings to native data types
    Indian_market_price = float(Indian_market_price_str)
    change = float(change)
    change_percnt = change_percnt.strip('()')  # Remove parentheses

    stock_current_time = time.time()

    result_data = {
        'stock_name': name,
        'Indian_Market_Price': Indian_market_price,
        'market_details': [{
            'marketchange': change,
            'marketchangepercent': change_percnt,
            'data_produced_timestamp': stock_current_time
        }]
    }
    driver.quit()

    return result_data


KAFKA_TOPIC_NAME_CONS = "jan29"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))






for i in range(0,20):
    urls=["SBIN.NS","HDFCBANK.NS","KOTAKBANK.NS","ICICIBANK.NS","RELIANCE.NS","TCS.NS","AXISBANK.NS","BAJFINANCE.NS","BRITANNIA.NS","ASIANPAINT.NS"]
    
    for url in urls:
        result_dict=scraped_data_india(url)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS,result_dict)
        print(result_dict)
       