import requests as r
import concurrent.futures as cf
from bs4 import BeautifulSoup
from ARBK_Scraper.utils import chunks, main_url, HEADERS
from ARBK_Scraper.db import business, engine


def _get_activity_codes():
    """
    Returns the list of activity codes by which the request for business lists is to be sent
    """
    main_get = r.get(main_url, headers=HEADERS)
    main_soup = BeautifulSoup(main_get.text, "html.parser")
    search_options = []
    for option in main_soup.find('select', attrs={'id': 'ddlnace'}).findAll('option'):
        if option["value"] != "Zgjedhe...":
            search_options.append(option["value"])
    return search_options


def _create_form(vs, ev, vsg, activity):
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


def _generate_business_list(activities_list):
    """
    For each activity code:
            -creates a session in which the request is saved;
            -Gets the Viewstate, Viewstategenerator and event validation values;
            -Sends a form using the sendofrm(function), which stores the data in the database.
    """
    for activity_no in activities_list:
        with r.Session() as s:
            req_1 = s.get(main_url, headers=HEADERS)
            soup = BeautifulSoup(req_1.content, "html.parser")
            # Hidden values
            view_state = soup.select_one('#__VIEWSTATE')["value"]
            view_state_gen = soup.select_one('#__VIEWSTATEGENERATOR')["value"]
            event_validation = soup.select_one('#__EVENTVALIDATION')["value"]
            # Data to be sent
            form_data = _create_form(
                vs=view_state,
                ev=event_validation,
                vsg=view_state_gen,
                activity=activity_no
            )
            s.post(main_url, headers=HEADERS, data=form_data)  # Send form data
            request = s.get("https://arbk.rks-gov.net/page.aspx?id=1,42")  # Send request to retrieve data
            soup = BeautifulSoup(request.text, "html.parser")
            headers = []
            for header in soup.findAll("th"):
                headers.append(header.find("span").text.strip())

            cnxn = engine.connect()
            for row in soup.find('table', attrs={'class': 'views-table cols-4'}).find('tbody').findAll("tr"):
                business_data = {}
                i_row = row.findAll('td', attrs={'class': 'views-field views-field-title name'})
                for i in range(len(i_row)):
                    business_data[headers[i]] = i_row[i].text.strip()
                business_data["link"] = "https://arbk.rks-gov.net/" + row.find('a').get('href')
                bus = business.insert().values(
                    Name=business_data["Emri"],
                    Trade_Name=business_data["Emri tregtar"],
                    Business_ID=business_data["Nr.Biznesit"],
                    Region=business_data["Komuna"],
                    Business_Type=business_data["Lloji Biznesit"],
                    Status=business_data["Statusi"],
                    Link=business_data["link"]
                )
                cnxn.execute(bus)
        print(f"Activity no. {activity_no} completed!")


def get_businesses(number_threads):
    """
    Concurrently generates list of businesses scraped from ARBK.
    :param number_threads: Number of threads
    """
    activities = _get_activity_codes()
    with cf.ThreadPoolExecutor() as executor:
        activities_gen = chunks(activities, number_threads)
        activity_chunks = [activities_gen.__next__() for _ in range(number_threads)]
        _ = executor.map(_generate_business_list, activity_chunks)
