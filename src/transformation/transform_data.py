import pandas as pd
import numpy as np
import re

def transform_sales_data(df_sales):
    """
    Cleans and transforms the sales data by handling duplicates, missing values, 
    converting columns to appropriate data types, and removing unnecessary columns.

    Parameters:
    df_sales (DataFrame): The DataFrame containing sales data that needs transformation.

    Returns:
    DataFrame: The cleaned and transformed DataFrame.
    """
    
    # Drop the 'Unnamed: 0' column if it exists, as it's often an index or unnecessary column.
    if 'Unnamed: 0' in df_sales.columns:
        df_sales = df_sales.drop('Unnamed: 0', axis=1)

    # Remove duplicate rows from the DataFrame.
    df_sales = df_sales.drop_duplicates()

    # Function to convert ratings from string to float. Handles cases where commas are used as decimal points.
    def clean_ratings(ratings):
        try:
            return float(ratings.replace(',', '.'))
        except:
            return np.NaN  # Returns NaN if conversion fails (e.g., missing or invalid values)

    # Function to convert the number of ratings from string to integers, removing commas and dots.
    def clean_no_ratings(no_ratings):
        try:
            return int(no_ratings.replace(',', '').replace('.', ''))
        except:
            return np.NaN  # Returns NaN for consistency with other missing values

    # Apply the cleaning functions to the 'ratings' and 'no_of_ratings' columns.
    df_sales['ratings'] = df_sales['ratings'].apply(clean_ratings).fillna(0)
    df_sales['no_of_ratings'] = df_sales['no_of_ratings'].apply(clean_no_ratings)

    # Clean the price columns: remove currency symbols and commas, then convert the result to float.
    df_sales['actual_price'] = df_sales['actual_price'].str.replace('₹', '').str.replace(',', '').replace('', np.NaN).astype('float')
    df_sales['discount_price'] = df_sales['discount_price'].str.replace('₹', '').str.replace(',', '').replace('', np.NaN).astype('float')

    # Handle missing values: Fill missing 'ratings' with 0.
    df_sales['ratings'] = df_sales['ratings'].fillna(0)

    # For rows with missing 'discount_price', fill it with the value from 'actual_price'.
    df_sales.loc[df_sales['discount_price'].isna(), 'discount_price'] = df_sales.loc[df_sales['discount_price'].isna(), 'actual_price']

    # Drop rows where 'actual_price' is missing, as it's essential for analysis.
    df_sales = df_sales.dropna(subset='actual_price')

    return df_sales  # Return the cleaned DataFrame


def transform_marketing_data(df_marketing):
    """
    Cleans and transforms the marketing data by handling duplicates, missing values, 
    converting weights to pounds, and standardizing price availability and condition.

    Parameters:
    df_marketing (DataFrame): The DataFrame containing marketing data that needs transformation.

    Returns:
    DataFrame: The cleaned and transformed DataFrame.
    """
    
    # Function to clean the 'prices.availability' column by standardizing certain values to 'YES' or 'NO'.
    def cleaning_unique_values_prices_availability(prices_availability):
        prices_availability = prices_availability.upper()  # Convert the text to uppercase for consistency
        yes_availabilty = ['YES', 'TRUE', 'SPECIAL ORDER', 'IN STOCK', '32 AVAILABLE', '7 AVAILABLE']
        no_availabilty = ['UNDEFINED', 'OUT OF STOCK', 'NO', 'MORE ON THE WAY', 'SOLD', 'FALSE', 'RETIRED']

        # If the value matches a 'yes' condition, return 'YES', otherwise return 'NO' for the 'no' condition.
        if prices_availability in yes_availabilty:
            return 'YES'
        elif prices_availability in no_availabilty:
            return 'NO'

        return prices_availability  # Return the original value if no match found.

    # Function to convert weight from a string format containing pounds and/or ounces into pounds as float.
    def convert_to_pounds(weight_str):
        try:
            pounds = 0
            ounces = 0

            # Conversion factor from ounces to pounds (1 pound = 16 ounces)
            ounces_to_pounds = 1 / 16

            # Extract the pounds from the string (if any), handling variations like 'lbs' or 'pounds'
            pounds_match = re.search(r'(\d*\.?\d+)\s?(?:lbs?|pounds?)', weight_str)
            if pounds_match:
                pounds = float(pounds_match.group(1))

            # Extract the ounces from the string (if any), handling variations like 'oz' or 'ounces'
            ounces_matches = re.search(r'(\d*\.?\d+)\s?(?:oz|ounces?)', weight_str)
            if ounces_matches:
                ounces = float(ounces_matches.group(1))

            # Convert ounces to pounds and add to the pounds value.
            total_pounds = pounds + (ounces * ounces_to_pounds)
            return total_pounds
        except:
            return np.NaN  # Return NaN if conversion fails.

    # Drop the 'Unnamed: 0' column if it exists, as it's often unnecessary.
    if 'Unnamed: 0' in df_marketing.columns:
        df_marketing = df_marketing.drop('Unnamed: 0', axis=1)

    # Remove duplicate rows from the DataFrame.
    df_marketing = df_marketing.drop_duplicates()

    # Apply data cleaning functions.
    df_marketing['weightInPounds'] = df_marketing['weight'].apply(convert_to_pounds)  # Convert weight to pounds.
    df_marketing['prices.availability'] = df_marketing['prices.availability'].apply(cleaning_unique_values_prices_availability)  # Standardize availability.
    df_marketing['prices.condition'] = df_marketing['prices.condition'].str.upper().str.replace('NEW OTHER (SEE DETAILS)', 'NEW')  # Clean and standardize condition.
    
    # Drop rows where 'prices.condition' contains unwanted values (the last 2 unique values).
    df_marketing = df_marketing[~(df_marketing['prices.condition'].isin(list(df_marketing['prices.condition'].unique()[-2:])))]

    # Clean the 'prices.shipping' column, removing 'USD' and converting it to integer.
    df_marketing['prices.shipping'] = df_marketing['prices.shipping'].fillna('free').apply(
        lambda x: 0 if 'USD' not in x else x.replace('USD', '').replace('.', '')
    ).astype('int')

    # Drop any columns containing 'UNNAMED' in their name and the 'weight' column.
    for col in df_marketing.columns:
        if 'UNNAMED' in col.upper():
            df_marketing = df_marketing.drop(col, axis=1)
        if col.upper() == 'WEIGHT':
            df_marketing = df_marketing.drop(col, axis=1)

    # Handle missing values: fill missing 'manufacturer' values with 'No Information'.
    df_marketing['manufacturer'] = df_marketing['manufacturer'].fillna('No Information')

    # Drop the 'ean' column if it exists, as it has more than 75% missing values.
    if 'ean' in df_marketing.columns:
        df_marketing = df_marketing.drop('ean', axis=1)

    return df_marketing  # Return the cleaned DataFrame


def transform_scraping_data(df_scraping):
    """
    Transforms and cleans the scraped data by handling missing values, filling them with default values,
    and dropping unnecessary columns.
    
    Parameters:
    df_scraping (DataFrame): The DataFrame containing the scraped data that needs transformation.

    Returns:
    DataFrame: The cleaned and transformed DataFrame.
    """
    
    # Fill missing values in the 'topik' column with 'Iklan' (advertisement), as the articles are considered ads (advertorials).
    df_scraping['topik'] = df_scraping['topik'].fillna('Iklan')

    # Fill missing values in the 'sub_topik' column with 'Belum ditentukan' (Not yet determined), indicating undefined subtopics.
    df_scraping['sub_topik'] = df_scraping['sub_topik'].fillna('Belum ditentukan')

    # Fill missing values in the 'topik_pilihan' column with 'Bukan topik pilihan' (Not a selected topic).
    df_scraping['topik_pilihan'] = df_scraping['topik_pilihan'].fillna('Bukan topik pilihan')

    # Fill missing values in the 'redaksi' (editorial) column with 'Anonim' (Anonymous), assuming no editorial attribution.
    df_scraping['redaksi'] = df_scraping['redaksi'].fillna('Anonim')

    # Fill missing values in the 'advetorial' column with 'Non Advertorial', indicating that the content is not an advertisement.
    df_scraping['advetorial'] = df_scraping['advetorial'].fillna('Non Advertorial')

    # Drop the 'topik_pilihan_link' column as it is not needed.
    df_scraping = df_scraping.drop('topik_pilihan_link', axis=1)

    # Drop duplicates data
    df_scraping = df_scraping.drop_duplicates()

    return df_scraping  # Return the cleaned DataFrame
