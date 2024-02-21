import pyodbc
import pandas as pd
from Config import server,db,uid,pwd


connDBR = pyodbc.connect(f'Driver=SQL Server;Server={server};Port =1433;Database={db};UID={uid};PWD={pwd}')
cursorR = connDBR.cursor()
query = """SELECT
	   CAST(RK1.[SHIFTDATE] as date) as Fact_Date 
	  ,RK1.[RESTAURANTNAME] as Restaurant
	  ,RK1.[ORDERCATEGORY] as Order_Category
	  ,CAST([PRINTAT___13] as date) as Open_Date
      ,CAST([PRINTAT___13] as time) as Open_Time
	  ,Cast([CLOSEDATETIME___37] as date) as Close_Date
      ,Cast([CLOSEDATETIME___37] as time) as Close_Time
	  ,RK1.[WAITER] as Main_Waiter
	  ,RK1.[ORDERNAME] as Invoice
	  ,[CATEGPATH] as Path_Category
	  ,[NAME1]	as Last_Folder 
	  ,[CATEGORY] as Category
      ,[CODE] as Code_Dish
      ,[DISH] as Dish
      ,[QUANTITY] as Quantity
	  ,RK1.[PRLISTSUM]/[QUANTITY] as Price
      ,RK1.[PRLISTSUM] as Sum_
	  ,[PAYSUM]-RK1.[PRLISTSUM] as Tax
	  ,[PAYSUM] as Total_Sum
	  ,[COMBODISH] as Combo
	  ,[CURRENCYTYPE] as Currency_Type
      ,[ORIGCURRENCY] as Currency
	  ,[F00000043]  as Food_Bar_Category
	  ,RK2.[TABLE] as Table_Num
      ,[GUESTCNTOLD] as Guest_Count     
  FROM [RK7N].[dbo].[VRK7CUBEVIEW1002] as RK1
  join [RK7N].[dbo].[VRK7CUBEVIEW1005] as RK2
  ON RK1.[ORDERNAME]=RK2.[ORDERNAME]
  WHERE CAST(RK1.[SHIFTDATE] as date) = DATEADD(day,-1,CAST(Getdate() as date));"""
df = pd.read_sql(query, connDBR)
connDB = pyodbc.connect(r'Driver={SQL Server};Server=HOME-PC;Database=PowerBi;Trusted_Connection=yes;')
cursor = connDB.cursor()
for index, row in df.iterrows():
     cursor.execute("INSERT INTO Ramstore_Sales (Fact_Date,Restaurant,Order_Category,Open_Date,Open_Time,Close_Date,Close_Time,Main_Waiter,Invoice,Path_Category,Last_Folder,Category,Code_Dish,Dish,Quantity,Price,Sum_,Tax,Total_Sum,Combo,Currency_Type,Currency,Food_Bar_Category,Table_Num,Guest_Count) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row.Fact_Date, row.Restaurant, row.Order_Category, row.Open_Date, row.Open_Time,row.Close_Date,row.Close_Time,row.Main_Waiter,row.Invoice,row.Path_Category,row.Last_Folder,row.Category,row.Code_Dish,row.Dish,row.Quantity,row.Price,row.Sum_,row.Tax,row.Total_Sum,row.Combo,row.Currency_Type,row.Currency,row.Food_Bar_Category,row.Table_Num,row.Guest_Count)
connDB.commit()
cursorR.close()
cursor.close()
print("Data was added successful")