import _sqlite3
import time
import requests as r
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import Table, Column, MetaData, Integer, String, create_engine

# Due to the large amount of data to be stored, the data is best stored in a database
# Here I use SQLite, but the script can be modified to use any database

engine = create_engine('sqlite:///KosovoBusinesses.db')
meta = MetaData()

business = Table(
    'Businesses', meta,
    Column('ID', Integer, primary_key=True),
    Column('Region', String),
    Column('Business_ID', Integer),
    Column('Type', String),
    Column('Class', String),
    Column('Link', String)
)

business_data = Table(  # These data are stored in Key-Value pairs due to differences in the number of fields among
    # businesses (e.g. closed businesses have different fields).
    'BusinessInfo', meta,
    Column('ID', Integer, primary_key=True),
    Column('Business_ID', Integer),
    Column('Key', String),
    Column('Value', String)
)

activity = Table(
    'Activities', meta,
    Column('ID', Integer, primary_key=True),
    Column('Business_ID', Integer),
    Column('Description', String),
    Column('Type', String)
)

meta.create_all(engine)
conn = engine.connect()

# We need a list of links with data for each business;
# On the page every business has its own page;
# There is no single page with a list of all businesses so a list has to be created.
# I figured out that if we search by Activity we can generate a list of all businesses with links to each
#   individual business.


# Therefore, the rest of the script is structured as follows:
#       - A list of activity codes by which to request lists of businesses is created;
#       - For each activity the main data and the link to the business is stored;
#       - For each business we scrap the page;


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept-Encoding': 'gzip,deflate,sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Connection': 'keep-alive'
}

conn2 = _sqlite3.connect('KosovoBusinesses.db')
cur = conn2.cursor()


# Activity codes by which to send requests

def activity_codes():
    """
    Returns the list of activity codes by which the request for business lists is to be sent
    """
    main_url = "https://arbk.rks-gov.net/"
    main_get = r.get(main_url, headers=HEADERS)
    main_soup = BeautifulSoup(main_get.text, "html.parser")
    search_options = []
    for option in main_soup.find('select', attrs={'id': 'ddlnace'}).findAll('option'):
        search_options.append(option['value'])
    return search_options


def create_form(vs, ev, vsg, activity):
    """
    Creates a form to be sent the main site to request a list of businesses for each activity.
    Requires the Activity code ,and __VIEWSTATE, __VIEWSTATEGENERATOR and __EVENTVALIDATION values.
    """
    hd = {
        "ctl00$ScriptManger1": "",
        "__EVENTTARGET": "",
        "__EVENTARGUMENT: ": "",
        "__EVENTARGUMENT:": "",
        "__VIEWSTATE": f"{vs}",
        "__VIEWSTATEGENERATOR": f"{vsg}",
        "__VIEWSTATEENCRYPTED": "",
        "__EVENTVALIDATION": f"{ev}",
        "query": "",
        "affiliate": "web-sdmg-uspto.gov",
        "ctl00$txtEmriBiznesit": "",
        "ctl00$txtNumriBiznesit": "",
        "ctl00$txtNumriFiskal": "",
        "ctl00$txtIDPronarit": "",
        "ctl00$ddlnace": f"{activity}",
        "ctl00$ddlnaceaktivitetetjera": "Zgjedhe...",
        "__ASYNCPOST": "true",
        "ctl00$Submit1": "Kërko"
    }
    return hd


def get_tables(table_soup):
    for row in table_soup.find('table', attrs={'class': 'views-table cols-4'}).find('tbody').findAll('tr'):
        i_row = row.findAll('td', attrs={'class': 'views-field views-field-title name'})
        bus = business.insert().values(
            Business_ID=i_row[3].string.strip(),
            Region=i_row[4].string.strip(),
            Type=i_row[5].string.strip(),
            Class=i_row[6].string.strip(),
            Link="https://arbk.rks-gov.net/" + row.find('a').get('href')
        )
        conn.execute(bus)


def business_list(activities_list):
    """
    For each activity code:
            -creates a session in which the request is saved;
            -Gets the Viewstate, Viewstategenerator and event validation values;
            -Sends a form using the sendofrm(function), which stores the data in the database.
    :param activities_list: A list of activity codes.
    :return: None! Adds data directly to the database.
    """
    main_url = "https://arbk.rks-gov.net/"
    for activity_no in activities_list:
        with r.Session() as s:
            req_1 = s.get(main_url, headers=HEADERS)
            soup = BeautifulSoup(req_1.content, "html.parser")
            # Hidden values
            viewstate = soup.select_one('#__VIEWSTATE')["value"]
            viewstategen = soup.select_one('#__VIEWSTATEGENERATOR')["value"]
            eventvalidation = soup.select_one('#__EVENTVALIDATION')["value"]
            # Data to be sent
            formData = create_form(vs=viewstate, ev=eventvalidation, vsg=viewstategen, activity=activity_no)
            # Send Form
            response = s.post(main_url, headers=HEADERS, data=formData)
            # Text
            response_2 = s.get("https://arbk.rks-gov.net/page.aspx?id=1,42")
            soup_2 = BeautifulSoup(response_2.text, "html.parser")
            # Get the data and insert to the database
            get_tables(soup_2)


# Now we have the list of links, so we can start scraping the individual pages.

def get_basic_info(soup, business_id):
    """
    Gets the main data of the business and stores them in the database.
    """
    basic_info = soup.find('table', attrs={'class': 'views-table cols-4'}).find('tbody')
    for row in basic_info.findAll('tr'):
        irow = row.findAll('td')
        if irow[0].find('b').string.strip() not in ['Emri i biznesit', 'Emri tregtar', 'Adresa', 'Telefoni', 'E-mail']:
            bus = business_data.insert().values(
                Business_ID=business_id,
                Key=irow[0].find('b').string.strip(),
                Value=irow[1].find('span').string.strip()
            )
            conn.execute(bus)


def get_activity(soup, business_id):
    """
    Stores the activity of each business in the database.
    """
    activities = soup.findAll('table')[4].find('tbody')
    for row in activities.findAll('tr'):
        irow = row.findAll('td')
        act = activity.insert().values(
            Business_ID=business_id,
            Description=irow[1].find('span').string.strip(),
            Type=irow[2].find('span').string.strip()
        )
        conn.execute(act)


def get_business_data(cur):
    """
    :param cur: The cursor to the database.
    """
    cur.execute("select * from businesses where Business_ID not in (select distinct Business_ID from Activities)")
    business_list_on_db = pd.DataFrame(cur.fetchall())
    for index, row in business_list_on_db.iterrows():
        link = row[7]
        Business_ID = row[4]
        req = r.get(link, headers=HEADERS)
        s = BeautifulSoup(req.content, "html.parser").find('div', attrs={'id': 'MainContent_pnl'})
        get_basic_info(soup=s, business_id=Business_ID)
        get_activity(soup=s, business_id=Business_ID)
        time.sleep(10)


if __name__ == '__main__':
    activities = activity_codes()
    business_list(activities)
    print("The list of businesses has been added to the database!")
    get_business_data(cur=cur)
    cur.close()
