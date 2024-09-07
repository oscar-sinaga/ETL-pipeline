import sys
import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import csv
from src.helper.db_connector_sales_data_raw import postgres_engine_sales_data

def extract_sales_data():
    """
    Extracts sales data from a PostgreSQL database and loads it into a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing sales data.
    """
    # Initialize the PostgreSQL engine
    engine = postgres_engine_sales_data()

    query = "SELECT * FROM amazon_sales_data;"

    try:
        # Using pandas to execute the SQL query and load data into a DataFrame
        df_sales = pd.read_sql(query, engine)
        print("Connection successful, and data loaded.")
        return df_sales
    except Exception as e:
        print("Connection failed:", e)
        return None


def extract_marketing_data():
    """
    Extracts marketing data from a CSV file and loads it into a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing marketing data.
    """
    try:
        df_marketing = pd.read_csv('data_source/marketing_data/ElectronicsProductsPricingData - ElectronicsProductsPricingData.csv')
        return df_marketing
    except Exception as e:
        print("Failed to load marketing data:", e)
        return None


def extract_scraping_data(pages=5, csv_filename='data_source/scraping_data/scraping_kompas.csv'):
    """
    Scrapes news articles from Kompas website and saves the results into a CSV file.

    Args:
        pages (int): Number of pages to scrape.
        csv_filename (str): Name of the CSV file to save the scraping results.

    Returns:
        pd.DataFrame: DataFrame containing the scraped news data.
    """
    # Configure logging
    logging.basicConfig(filename='data_source/scraping_data/scraping.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    RED = "\033[91m"  # Red color for errors

    # CSV headers
    fieldnames = ['judul', 'topik', 'sub_topik', 'topik_pilihan', 'tanggal_waktu_publish', 'redaksi', 'advetorial', 'isi_berita', 'link', 'topik_pilihan_link']

    # Open the CSV file in append mode and write the header if the file is new
    with open(csv_filename, mode='a', newline='', encoding='utf-8') as file:
        # Sign the scrapping process begin
        print(f"========== Scraping data kompas begin ==========")
        
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if file.tell() == 0:  # Write header only if the file is new
            writer.writeheader()

        # Loop through each page to scrape articles
        for i in range(1, pages + 1):
            try:
                url = f"https://indeks.kompas.com/?site=all&page={i}"
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                links = [link.get('href') for link in soup.find_all('a', class_='article-link')]

                # Loop through each article link
                for j, link in enumerate(links):
                    try:
                        response_news = BeautifulSoup(requests.get(link).text, 'html.parser')

                        # Scrape advertorial information
                        advetorial = response_news.find('div', class_='kcm__header__advertorial')
                        advetorial = advetorial.get_text() if advetorial else ''

                        # Scrape breadcrumb topics
                        topic_tags = response_news.find_all('li', class_='breadcrumb__item')
                        topics = [tag.find('span').get_text() for tag in topic_tags]
                        topik = topics[1] if len(topics) > 1 else ''
                        sub_topik = topics[2] if len(topics) > 2 else ''

                        # Scrape optional topic link
                        topik_pilihan = ''
                        topik_pilihan_link = ''
                        if response_news.find('div', class_='topicSubtitle'):
                            topik_pilihan = response_news.find('div', class_='topicSubtitle').find('a').get_text()
                            topik_pilihan_link = response_news.find('div', class_='topicSubtitle').find('a').get('href')

                        # Scrape article title
                        judul = response_news.find('h1', class_='read__title')
                        judul = judul.get_text() if judul else ''

                        # Scrape publication date and time
                        tanggal_waktu_publish = response_news.find('div', class_='read__time')
                        tanggal_waktu_publish = tanggal_waktu_publish.get_text().split(' - ')[1] if tanggal_waktu_publish else ''

                        # Scrape author or editor names
                        redaksi_tag = response_news.find('div', class_='credit-title-name')
                        redaksi = ' '.join([penulis.get_text() for penulis in redaksi_tag.find_all('h6')]) if redaksi_tag else ''

                        # Scrape article content
                        konteks_tag = response_news.find('div', class_='read__content')
                        isi_berita = ' '.join([konteks.get_text() for konteks in konteks_tag.find_all('p')]) if konteks_tag else ''

                        # Store the scraped result into a dictionary
                        result = {
                            'judul': judul,
                            'topik': topik,
                            'sub_topik': sub_topik,
                            'topik_pilihan': topik_pilihan,
                            'tanggal_waktu_publish': tanggal_waktu_publish,
                            'redaksi': redaksi,
                            'advetorial': advetorial,
                            'isi_berita': isi_berita,
                            'link': link,
                            'topik_pilihan_link': topik_pilihan_link
                        }

                        # Write the result to the CSV file
                        writer.writerow(result)

                        # Random delay to avoid getting blocked
                        time.sleep(random.uniform(0.1, 1))

                    except Exception as e:
                        print(f'{RED}ERROR - page {i} link {j + 1} = {link}')
                        logging.error(f'ERROR - page {i} link {j + 1} = {link}')
                        time.sleep(random.uniform(0.1, 1))

            except Exception as e:
                print(f'{RED}ERROR - page {i}')
                logging.error(f'ERROR - page {i}')
                continue
    
    # Sign the scrapping process ended
    print(f"========== Scraping data kompas end then return the dataframe ==========")
    # Load the scraped data into a DataFrame
    df_scraping = pd.read_csv(csv_filename)
    return df_scraping
