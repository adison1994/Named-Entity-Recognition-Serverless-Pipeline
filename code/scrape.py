
import boto3
from boto3.dynamodb.conditions import Key, Attr
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from time import sleep
import re
from selenium.webdriver.chrome.options import Options
import time

region = ''
s3_client = boto3.resource('s3', region_name = region)


def open_browser(alt_user_name = 'Thank you for your website'):
    opts = Options()
    opts.add_argument("user-agent=" + str(alt_user_name))
    path = 'chromedriver.exe'
    return webdriver.Chrome(executable_path = path, options=opts)


def scrape_earnings(url):
	path = '/usr/bin/chromedriver'
	alt_user_name = ''

	chrome_options = webdriver.ChromeOptions()
	chrome_options.add_argument('--headless')
	chrome_options.add_argument('--disable-gpu')
	chrome_options.add_argument('--no-sandbox')
	chrome_options.add_argument('--ignore-certificate-errors')
	chrome_options.add_argument("user-agent=" + str(alt_user_name))
	print('Start')
	browser = webdriver.Chrome(executable_path = path, options=chrome_options)
	browser.get(url)
	print('1-1')
	soup    = BeautifulSoup(browser.page_source, 'html.parser')
	article = soup.find('article')
	print('1-2')
	#print(article)
	text_ = [item.text for 
			item in article.find_all('p')]
	#print(text_)
	text_ = text_[5:17]
	print('1-3')
	browser.close()
	timestr = time.strftime("%Y%m%d-%H%M%S")
	file_name = 'earnings_transcript_' + timestr + '.txt'
	with open(file_name, 'w', encoding='utf8') as transcript:
		for text in text_:

			transcript.write(text)
			transcript.write('\n')
	transcript.close()

	return file_name


def scrape_transcripts(url : str):
	file_ = scrape_earnings(url)
	response = s3_client.Object('ner-recognized-entites','scraper_output/{}'.format(file_)).upload_file(Filename=file_)
	
	return file_
