from bs4 import BeautifulSoup
import requests
from Utils import *
from LaptopLogger import logging
from datetime import datetime

class Flipkart_Scraper:
    def __init__(self , DataBase_Name , Table_Name, Common_Table_Name ,User_Name):
        # Initialize scraper with database and user details
        self.Row_Count = 0
        self.Count=3
        self.Utils_Object = Utils
        self.Max_Pages = 100
        self.DataBase_Name = DataBase_Name
        self.Table_Name = Table_Name
        self.Common_Table_Name = Common_Table_Name
        # Establish connection to the MySQL database
        self.Database_Connection, self.Database_Cursor = self.Utils_Object.Connect_To_Mysql_Database(self.DataBase_Name)
        self.Insert_Date = f"{datetime.now().strftime('%Y-%m-%d-%H')}"
        self.Insert_User = User_Name
        self.Load_Type = "FullLoad"
 

    
    def Main(self):

        try:

            # Logging the start of the full load process
            logging.info("Starting FullLoad Process")

            # Create tables if they do not exist
            Create_IF_Not_Exists =self.Utils_Object.Create_Table(Database_Cursor = self.Database_Cursor ,Database_Connection = self.Database_Connection , Table_Name = self.Table_Name)
            Create_IF_Common_table_Not_Exists =self.Utils_Object.Create_Table(Database_Cursor = self.Database_Cursor ,Database_Connection = self.Database_Connection , Table_Name = self.Common_Table_Name)
            logging.info("Table name :- "+Create_IF_Not_Exists)
            logging.info("Common Table name :- "+Create_IF_Common_table_Not_Exists)

            # Loop through pages to scrape data
            for Page_Number in range(0,self.Max_Pages): 
                # Construct URL for the current page
                Url = "https://www.flipkart.com/search?q=laptop&page="+ str(Page_Number)
                
                # Get raw list of products data from the page
                Raw_List_Of_Products_Data = self.Get_Raw_List_Of_Products_Data(Url = Url)
                 
                # Check if scraping should stop after a certain number of failed attempts
                if self.Count == 0:
                    
                    logging.info("Rows Inserted "+ str(self.Row_Count) + " Number of pages Scraped " + str(Page_Number))

                    logging.info("Ending Fullload Process")


                    return "Rows Inserted "+ str(self.Row_Count) + " Number of pages Scraped " + str(Page_Number)
                # Continue if no data is found on the page
                if len(Raw_List_Of_Products_Data) == 0:
                    self.Count -= 1
                    continue
                    
                # Process and save data for each product
                for Raw_Each_Product_Data in Raw_List_Of_Products_Data:
                    # Build processed laptop data from raw data
                    Processed_Laptops_Data = self.Build_Process_Laptop_Data_From_Raw_Data(Raw_Each_Product_Data)
                    
                    # Save processed data to both specific and common tables
                    self.Utils_Object.Save_Laptop_Data(Database_Connection = self.Database_Connection , Database_Cursor = self.Database_Cursor , Table_Name= self.Table_Name , Data = Processed_Laptops_Data)
                    self.Utils_Object.Save_Laptop_Data(Database_Connection = self.Database_Connection , Database_Cursor = self.Database_Cursor , Table_Name= self.Common_Table_Name , Data = Processed_Laptops_Data)
                    
                    self.Row_Count += 1
        except Exception as e :
            logging.error("Error found in main function in Flipkart_Main.py"+str(e),exc_info=True)
        
        

           
    def Get_Raw_List_Of_Products_Data(self,Url):
        try:
            # Send request to the URL
            Request_Data =requests.get(Url)
            
            # Parse the content using BeautifulSoup
            Soup=BeautifulSoup(Request_Data.content,'html.parser')
            
            # Find all product data elements
            Raw_List_Of_Products_Data = Soup.find_all(class_="yKfJKb")
            
            return Raw_List_Of_Products_Data
        
        except Exception as e:
        
            logging.error("Error found in Get_Raw_List_Of_Products_Data  function in Main.py :- "+str(e),exc_info=True)
        

    def Build_Process_Laptop_Data_From_Raw_Data(self,Raw_Each_Product_Data):
        try:
            # Initialize a dictionary to store processed product data
            Output_Data_Dictionary = {"Title":" ","Rating":" ","Total_Number_Of_Customer_Rated":" ","Processor":" ","RAM":" ","Operating_System":" ","Display":" ","Memory":" "
                        ,"Warranty":" ","Others":" ","Prices":"","Insert_Date":"","Insert_User":"","Load_Type":""}            
            Words = ''
            Others = ''
            
            # Extract price data
            Get_Price_Data = Raw_Each_Product_Data.find("div",{'class':'Nx9bqj'})        
            Price_Data = Get_Price_Data.text[1:] if Get_Price_Data is not None else " "
            Output_Data_Dictionary['Prices'] = Price_Data

            # Extract rating data
            Get_Rating_Data = Raw_Each_Product_Data.find("div",{'class':'XQDdHH'})
            Rating_Data = Get_Rating_Data.text if Get_Rating_Data is not None else ""
            Output_Data_Dictionary['Rating'] = Rating_Data

            # Extract the total number of customer ratings
            Get_Total_Number_Of_Customer_Rated_Data=Raw_Each_Product_Data.find("div",class_='Wphh3N')
            Total_Number_Of_Customer_Rated_Data = Get_Total_Number_Of_Customer_Rated_Data.text.split(" ") if Get_Total_Number_Of_Customer_Rated_Data is not None else " "
            Output_Data_Dictionary['Total_Number_Of_Customer_Rated']=Total_Number_Of_Customer_Rated_Data
            
            # Extract title data
            Get_Title_Data = Raw_Each_Product_Data.find("div",class_='KzDlHZ')
            Title_Data = Get_Title_Data.text if Get_Title_Data is not None else " "
            Output_Data_Dictionary['Title'] = Title_Data
            
            # Extract other product details
            Get_Description_Data = Raw_Each_Product_Data.find_all('li',{'class':'J+igdf'})
                

            for itr in Get_Description_Data:
                if "Processor" in itr.text:
                    Processor_Data = itr.text if itr is not None else ""
                    Output_Data_Dictionary["Processor"] = Processor_Data
                elif "RAM" in itr.text:
                    RAM_Data = itr.text if itr is not None else ""
                    Output_Data_Dictionary['RAM'] = RAM_Data
                    continue
                elif "Operating System" in itr.text:
                    OS_Data = itr.text if itr is not None else ""
                    Output_Data_Dictionary["Operating_System"] = OS_Data
                    continue
                elif "Display" in itr.text:
                    Display_Data = itr.text if itr is not None else ""
                    Output_Data_Dictionary["Display"] = Display_Data
                    continue
                elif "Warranty" in itr.text:
                    Warranty_Data = itr.text if itr is not None else ""
                    Output_Data_Dictionary["Warranty"] = Warranty_Data
                    continue
                elif "SSD"  in itr.text:
                    Memory_Data = itr.text if itr is not None else ""
                    Output_Data_Dictionary["Memory"] = Memory_Data
                    continue
                elif "HDD" in itr.text:
                    Memory_Data=itr.text if itr is not None else ""
                    Output_Data_Dictionary["Memory"] = Memory_Data
                    continue
                else:
                    Words = itr.text if itr is not None  else ""
                    Others = Others + Words + " , "
           
            # Set additional data fields
            Output_Data_Dictionary['Others']=Others
            Output_Data_Dictionary['Load_Type']=self.Load_Type
            Output_Data_Dictionary['Insert_User']=self.Insert_Date 
            Output_Data_Dictionary['Insert_Date']=self.Insert_Date
            
            return Output_Data_Dictionary       
        except Exception as e:
            logging.error("Error found in Build_Process_Laptop_Data_From_Raw_Data  function in Main.py :- "+str(e),exc_info=True)
