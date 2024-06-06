from LaptopScraper.Flipkart_Scrapper import FullLoad_Flipkart_Main,IncreamentalLoad_Flipkart_Main
from LaptopLogger import logging

def Scrapper_Main():
    try:
        User_Name = "Karthik"
        
        Flipkart_Scraper_Obj = IncreamentalLoad_Flipkart_Main.Flipkart_Scraper(DataBase_Name = "Laptop_Data" , Table_Name = "Flipkart_Laptop_Data",Common_Table_Name = "Laptop_Data", User_Name = User_Name)

        print(Flipkart_Scraper_Obj.Main())         

    except Exception as e:
        logging.error("Error found in Flipkart_Main.py :- "+str(e),exc_info=True)