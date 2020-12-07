from bs4 import BeautifulSoup
from selenium import webdriver
import os
import re
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from crawl import crawl_site
from time import sleep
from selenium.webdriver.chrome.options import Options
import time


# Scraping all
# The format used for AWS lambda functions
def perform_scrape(event=None, context=None):

    # defining the chrome webdriver for selenium
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    
    driver = webdriver.Chrome(chrome_options=chrome_options,executable_path='chromedriver.exe')
    url=''
    region = ''
    s3_client = boto3.resource('s3', region_name = region)
    file_ = crawl_site(url,driver)
    response = s3_client.Object('ner-recognized-entites','scraper_output/{}'.format(file_)).upload_file(Filename=str(file_))
    driver.quit()

    return {
        'statusCode': 200,
        'body': json.dumps(file_)
    } 

if __name__ == "__main__":
    print(perform_scrape())
