import itertools
import pymysql


class Utils:

    @staticmethod
    def Connect_To_Mysql_Database(DataBase_Name):
        # Establish a connection to the MySQL database
        Database_Connection = pymysql.connect(
            host='localhost',  # Hostname of the database server
            user='root',  # Username for the database
            password="karthiksara@2123",  # Password for the database
            db=DataBase_Name  # Name of the database
        )
        # Create a cursor object using the connection
        Database_Cursor = Database_Connection.cursor()
        # Return the connection and cursor objects
        return Database_Connection, Database_Cursor
        
    
    @staticmethod
    def Create_Table(Database_Cursor , Database_Connection,Table_Name):
        # Create the cursor object again using the connection
        Database_Cursor=Database_Connection.cursor()
        # Execute the SQL command to create a table if it does not exist
        Database_Cursor.execute('CREATE TABLE IF NOT EXISTS '+Table_Name +
                                ' (Title VARCHAR(8955),Processor  VARCHAR(255),RAM VARCHAR(255),Operating_System  VARCHAR(255),Memmory  VARCHAR(255),Warranty Varchar(255),Others Text,Prices  VARCHAR(255), Load_Type  VARCHAR(255) , Insert_Date  VARCHAR(255) , Insert_User  VARCHAR(255),Rating Varchar(255),count_rating Varchar(255))')
        # Commit the changes to the database
        Database_Connection.commit()
        # Return a message indicating the table creation status
        return "Table Created Sucessfully Or Table Allready exists"
    
    @staticmethod
    def Save_Laptop_Data(Database_Cursor , Database_Connection , Table_Name  , Data):
        # SQL command to insert data into the table
        sql = "INSERT INTO  "+ Table_Name +"(Title,Processor,RAM,Operating_System,Memmory,Warranty,Others,Prices ,Load_Type , Insert_Date , Insert_User,Rating, count_rating ) VALUES (%s, %s,%s, %s,%s, %s,%s,%s,%s,%s,%s,%s,%s)"
        # Tuple of values to be inserted, extracted from the Data dictionary
        val = (str(Data['Title']),str(Data['Processor']),str(Data['RAM']),str(Data['Operating_System']),str(Data['Memory']),str(Data['Warranty']),str(Data['Others']),str(Data['Prices']) , str(Data['Load_Type']) , str(Data['Insert_Date']), str(Data['Insert_User']),str(Data['Rating']),str(Data['Total_Number_Of_Customer_Rated'])) 
        # Execute the SQL command with the provided values
        Database_Cursor.execute(sql, val)
        # Commit the changes to the database
        Database_Connection.commit()

    @staticmethod
    def Get_Title(Database_Cursor , Database_Connection , Table_Name):
        # SQL command to select all titles from the specified table
        SQL_Get_Title = "SELECT Title FROM "+Table_Name
        
        Database_Cursor.execute(SQL_Get_Title)
        # Fetch all results from the executed command
        Title_Data = Database_Cursor.fetchall()
        # Flatten the list of tuples into a single list of titles
        Title_Data_List = list(itertools.chain(*Title_Data))
        
        # Return the list of titles
        return Title_Data_List


#Getting Title
# DataBase_Name = "ecommerce_laptop_product_scraper"
# Table_Name = "Flipkart_Table"
# Utils_Object = Utils()
# Database_Connection, Database_Cursor = Utils_Object.Connect_To_Mysql_Database(DataBase_Name)    
# Title_Data = Utils_Object.Get_Title(Database_Cursor = Database_Cursor , Database_Connection = Database_Connection, Table_Name = Table_Name)
# print(Title_Data)

# #Creating a table
# DataBase_Name = "ecommerce_laptop_product_scraper"
# Table_Name = "Flipkart_Table"
# Utils_Object = Utils()
# Database_Connection, Database_Cursor = Utils_Object.Connect_To_Mysql_Database(DataBase_Name)    
# Creating_a_table = Utils_Object.Create_Table(Database_Connection = Database_Connection , Database_Cursor = Database_Cursor, Table_Name=Table_Name)
# print(Creating_a_table)

# # Inserting a row into the table
# Data = {"Title":"Dell","Rating":"3.4","Total_Number_Of_Customer_Rated":"400","Processor":"Intel Core i7 Processor (11th Gen)","RAM":"16GB","Operating_System":"Windows 10","Display":"512 GB SSD","Memory":" "
#                         ,"Warranty":"1 Year Warranty","Others":"Office Home & Student 2021 , ","Prices":"34,899","Insert_Date":"2024-05-14-11","Insert_User":"Karthik","Load_Type":"IncreamentalLoad"}
# DataBase_Name = "ecommerce_laptop_product_scraper"
# Table_Name = "Flipkart_Table"
# Utils_Object = Utils()
# Database_Connection, Database_Cursor = Utils_Object.Connect_To_Mysql_Database(DataBase_Name)    
# Inserting_A_Row_Data = Utils_Object.Save_Laptop_Data(Database_Connection = Database_Connection , Database_Cursor = Database_Cursor, Table_Name = Table_Name,Data = Data)
# print(Inserting_A_Row_Data)