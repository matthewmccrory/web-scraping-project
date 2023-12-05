import numpy as np
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re

def fix_rates(row):
    return re.sub(r'[£,pa*¹]', '', row).rstrip()

def fix_broadband(row):
    return row.split(':')[-1]
    
def get_house_data():
    base_url = "https://www.propertypal.com/property-for-sale/belfast" # base url will be used for iterating over each page
    other_url = "https://www.propertypal.com/" # used for concatenating with link to each individual house
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.9999.99 Safari/537.36" # need user agent to get through to property pal website
    headers = {"User-Agent": user_agent}

    # Dictionary including areas of belfast ------ Used to enrich data + add to analysis
    areas_of_belfast = {'BT1': 'Central',
                        'BT2': 'Central',
                        'BT3': 'North-East',
                        'BT4': 'East',
                        'BT5': 'East',
                        'BT6': 'South-East',
                        'BT7': 'South',
                        'BT8': 'South',
                        'BT9': 'South',
                        'BT10': 'South-West',
                        'BT11': 'West',
                        'BT12': 'West',
                        'BT13': 'North',
                        'BT14': 'North-West',
                        'BT15': 'North',
                        'BT16': 'East',
                        'BT17': 'West'
                        }

    page_nums = []

    # getting the final page number
    response = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    page_numbers = soup.find_all('a', class_ = 'sc-1829moy-3 dWFqqj') # find all a tags with that class

    final_df = pd.DataFrame()

    for num in page_numbers: # iterate through <a tags with that class, appending each text value to the empty list
        page_nums.append(num.text)

    final_page = int(page_nums[-1]) # take the last element of the list which we know to be the final page

    try:

        for page in range(1, final_page+1):
            print(f'Page number:{page}')
            page_url = f'{base_url}/page-{page}' # uses url + page number to flick between pages
            response = requests.get(page_url, headers=headers) # get the response, cant forget user agent
            soup = BeautifulSoup(response.text, 'lxml')

            house_links = soup.find_all('a', class_="sc-rof2h7-2 ZIDza")
            for link in house_links: # getting all the different links to each house on the page

                # empty house_df and details_df
                house_df = pd.DataFrame()
                details_df = pd.DataFrame()
                key = None
                value = None
                house_info_dict = {}

                # create empty lists to store data
                prices = []
                addresses = []
                full_postcodes = []
                postcode_numbers = []
                estate_agents = []

                try:
                    house_url = link['href'] # path to get link for each house
                    print(house_url)
                    full_url = other_url + house_url
                    house_response = requests.get(full_url, headers = headers) # individual response for each house which I will then iterate through
                    house_soup = BeautifulSoup(house_response.text, 'lxml')
                except Exception as e:
                    print('Error with house url:{e}')

                # Getting price, address, town and estate agent data --- include exception handling to get set each field = to na in the event data can not be captured
                try:
                    price = house_soup.find('p', class_='sc-x8w5jm-0 hCLmRR pp-property-price').span.text.split(' ')[-1]  # gets price with asking price attached
                    price = re.sub(r'[£,]', '', price)
                    price = int(price)
                except Exception as e:
                    price = np.nan
                try:
                    address = house_soup.find('h1', class_='sc-11tz8h0-0 ckZmwy').text  # gets address
                    if address.endswith(','):
                        address = address[:-1]
                except Exception as e:
                    address = np.nan
                try:
                    full_postcode = house_soup.find('p', class_='sc-11tz8h0-5 drredi').text.split(',')[-1]  # gets postcode - potentially introduce code to ensure postcode starts with 'BT'
                    if full_postcode:
                        postcode_number = full_postcode.lstrip().split(' ')[0]
                    else:
                        postcode_number = np.nan
                except Exception as e:
                    full_postcode = np.nan

                try:
                    estate_agent = house_soup.find('h3', class_='sc-1898sr3-15 hjDZdE').text.replace('Contact ', '')  # gets estate agent
                except Exception as e:
                    estate_agent = np.nan

                prices.append(price)
                addresses.append(address)
                full_postcodes.append(full_postcode)
                postcode_numbers.append(postcode_number)
                estate_agents.append(estate_agent)

                # create house dataframe
                house_df = pd.DataFrame({'Price (£)':prices, 'Address':addresses, 'Postcode':full_postcodes, 'Postcode_number':postcode_numbers, 'Estate Agent': estate_agents})

                try:

                    house_info = house_soup.find('table', class_='sc-1mbtuea-0 gjcVzr pp-key-info')  # access table that stores key information

                    for row in house_info.find_all('tr', class_='sc-1mbtuea-2 dfVsPi pp-key-info-row'):

                        # Find the key and value elements in the row
                        key_element = row.find('p', class_='sc-11tz8h0-5 bUgPNk', attrs={'font-weight': '600'})  # get the key e.g price
                        value_element = row.find('p', class_='sc-11tz8h0-5 hnrgBt')  # get the value e.g £450,000

                        # Extract text from the elements
                        if key_element:
                            key = key_element.get_text(strip=True)

                        if value_element:
                            value = value_element.get_text(strip=True)

                        # Print the key-value pair
                        if key_element and value_element:  # if a key and element exist append them to the dictionary
                            house_info_dict[key] = value

                    # Create a new dictionary with lists of values
                    reformatted_dict = {key: [value] for key, value in house_info_dict.items()}  # makes the value a list to avoid an index error

                    # Convert the reformatted dictionary to a DataFrame
                    details_df = pd.DataFrame(reformatted_dict) # creates details dataframe
                    print(f'details df: {details_df}')

                    complete_df = pd.concat([house_df,details_df], axis=1) # join house and details dfs together before we join to master dataframe
                    print(f'complete_df: {complete_df}')
                    all_columns = list(set(final_df.columns).union(complete_df.columns))  # important line of code ** gets the unique columns from both dataframes (set), joins them using (union) and puts them into a (list)  --> then reindex to include all of these columns in the complete_df, where there is no value np.nan will be the value.
                    complete_df = complete_df.reindex(columns=all_columns, fill_value=np.nan) # reindex complete dataframe to include all columns from final_df also
                    final_df = final_df.reindex(columns=all_columns, fill_value=np.nan) # reindex final_df to include all columns from complete_df also

                    final_df = pd.concat([final_df, complete_df], axis=0)
                    # drop typical mortgage column

                except Exception as e:
                    print(f'Error collecting house_info data: {e}')

    except Exception as e:
        print(f'Error getting price/address/city/estate agent:{e}')

    # Editing the final dataframe
    #editing index
    try:
        final_df.reset_index(drop=True, inplace=True)
        final_df.index = final_df.index
    except Exception as e:
        print(f'Error with index: {e}')

    # Using re module to edit rates values also extracting just 900mbps from broadband
    try:
        final_df['Rates'] = final_df['Rates'].apply(lambda x: fix_rates(x) if pd.notna(x) else x)
        final_df['Broadband'] = final_df['Broadband'].apply(lambda x: fix_broadband(x) if pd.notna(x) else x)
    except Exception as e:
        print('Error editing Rates/Broadband column: {e}')

    # Dropping typical mortgage column
    final_df_columns = list(final_df.columns)
    if 'Typical Mortgage' in final_df_columns:
        final_df.drop(columns=['Typical Mortgage'], inplace=True)

    # Creating new Belfast district using postcode number
    try:
        final_df['Belfast District'] = final_df['Postcode_number'].map(areas_of_belfast)
    except Exception as e:
        print(f'Error mapping areas: {e}')

    try:
        final_df.to_parquet('final_df.parquet')
    except Exception as e:
        print(f'Error saving file as parquet: {e}')


if __name__ == '__main__':
    get_house_data()












