from ARBK_Scraper import business_list, business_data
from ARBK_Scraper.db import create_tables


if __name__ == "__main__":
    create_tables()
    business_list.get_businesses(8)
    business_data.get_all_data(8)
