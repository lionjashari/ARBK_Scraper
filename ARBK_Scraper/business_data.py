from ARBK_Scraper.db import business_data, engine, activity
from ARBK_Scraper.utils import HEADERS, chunks
from bs4 import BeautifulSoup
import concurrent.futures as cf
import requests as r
from datetime import datetime


cnxn = engine.connect()


def _get_basic_info(soup, business_id):
    """
    Gets the main data of the business and stores them in the database.
    """
    basic_info = soup.find('table', attrs={'class': 'views-table cols-4'}).find('tbody')
    for row in basic_info.findAll('tr'):
        i_row = row.findAll('td')
        if i_row[0].find('b').string.strip() not in ['Emri i biznesit', 'Emri tregtar', 'Adresa', 'Telefoni', 'E-mail']:
            bus = business_data.insert().values(
                Business_ID=business_id,
                Key=i_row[0].find('b').string.strip(),
                Value=i_row[1].find('span').string.strip()
            )
            cnxn.execute(bus)


def _get_activity(soup, business_id):
    """
    Stores the activity of each business in the database.
    """
    activities = soup.findAll('table')[4].find('tbody')
    for row in activities.findAll('tr'):
        i_row = row.findAll('td')
        act = activity.insert().values(
            Business_ID=business_id,
            Description=i_row[1].find('span').string.strip(),
            Type=i_row[2].find('span').string.strip()
        )
        cnxn.execute(act)


def _get_business_data(list_data):
    i = 0
    for business_id, link in list_data:
        req = r.get(link, headers=HEADERS)
        s = BeautifulSoup(req.content, "html.parser").find('div', attrs={'id': 'MainContent_pnl'})
        _get_basic_info(soup=s, business_id=business_id)
        _get_activity(soup=s, business_id=business_id)
        i += 1
        if i % 1000 == 0:
            print(f"1000 completed at time {datetime.now()}.")


def get_all_data(number_threads):
    """
    Concurrently sends requests to the data
    """
    business_list_on_db = cnxn.execute('''select * from "Businesses" where "Businesses"."Business_ID" not in 
        (select distinct "Business_ID" from "BusinessInfo")''')
    data = []
    for row in business_list_on_db:
        data.append((row["Business_ID"], row["Link"]))

    with cf.ThreadPoolExecutor() as executor:
        activities_gen = chunks(data, number_threads)
        activity_chunks = [activities_gen.__next__() for _ in range(number_threads)]
        _ = executor.map(_get_business_data, activity_chunks)
