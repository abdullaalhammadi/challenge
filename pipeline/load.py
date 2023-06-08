
import pandas as pd
import sqlite3
from aggregates import (aggregates,
                        aggregate_tables,
                        monthly_volume)

from data_import import (final,
                         weather_transaction,
                         users,
                         sales)


monthly_volume["order_date"] = monthly_volume["order_date"].astype(str)

con = sqlite3.connect("../transasctions.db")
cur = con.cursor()

cur.execute('''

CREATE TABLE IF NOT EXISTS
  "sales_customers" (
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "product_id" INTEGER,
    "quantity" INTEGER,
    "price" REAL,
    "order_date" TIMESTAMP,
    "name" TEXT,
    "username" TEXT,
    "email" TEXT,
    "lat" REAL,
    "lon" REAL
  )

''')
            
cur.execute('''
CREATE TABLE IF NOT EXISTS
  "sales" (
    "order_id" INTEGER,
    "customer_id" INTEGER,
    "product_id" INTEGER,
    "quantity" INTEGER,
    "price" REAL,
    "order_date" TIMESTAMP
  )
  
''')
            
cur.execute('''
CREATE TABLE IF NOT EXISTS
  "weather" (
    "order_id" INTEGER,
    "weather_main" TEXT,
    "main_temp" REAL,
    "main_humidity" INTEGER,
    "wind_speed" REAL
  )
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS
  "customers" (
    "customer_id" INTEGER,
    "name" TEXT,
    "username" TEXT,
    "email" TEXT,
    "lat" REAL,
    "lon" REAL
  )
''')
            
final.to_sql("sales_customers", con, if_exists='append', index=False)
weather_transaction.to_sql("weather", con, if_exists='append', index=False)
users.to_sql("customers", con, if_exists='append', index=False)
sales.to_sql("sales", con, if_exists='append', index=False)
pd.DataFrame(sales["product_id"].drop_duplicates()).to_sql("products", con, if_exists='replace', index=False)

k = 0

for i in aggregates:
    i.to_sql(aggregate_tables[k], con, if_exists='replace', index=False)
    k += 1

cur.close()
