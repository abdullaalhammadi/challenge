
## The only packages we would need for this pipeline are `Requests`, `os`, `Pandas`, `SQLite`, and `Streamlit`.
## Requests is to make HTTP requests to fetch the data from REST API endpoints
## And Pandas is for data transformations and wrangling.

import requests
import pandas as pd
import os
import json
import re
import sqlite3
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

# import random as r

api_key = os.getenv('API_KEY')
print("The API KEY is: " + api_key)
sales = pd.read_csv("./data/sales_data.csv", parse_dates=["order_date"], index_col=False)

# I improvised this script to re-assign the order ID's in case a duplicate exists.
# It finds the duplicate values and iterates by row, where if the value of the order_id
# Of the row exists in the list of duplicated value, it generates a random id
# And if that generated id is not already in use, replaces the duplicated id with it.

# dupes = (sales[sales["order_id"]
#                .duplicated(keep=False)]
#                .sort_values("order_id", ignore_index=True)
#                ["order_id"]
#                .tolist())

# for index, row in sales.iterrows():
#     if row["order_id"] in dupes:
#         new_id = r.randint(1000, 9999)

#         if new_id not in sales["order_id"].tolist():
#             sales.loc[index, "order_id"] = new_id

users = (pd.json_normalize(requests
            .get("https://jsonplaceholder.typicode.com/users")
            .json(), sep="_")[["id",
                               "name",
                               "username",
                               "email",
                               "address_geo_lat",
                               "address_geo_lng"]]
                                   .rename(
                                       columns=
                                       {
                                            "id": "customer_id",
                                            "address_geo_lat": "lat",
                                            "address_geo_lng": "lon"
                                    }
                                )
                            )

users[["lat", "lon"]] = (users[["lat", "lon"]]
                         .astype(float))

final = (sales
          .merge(users, on='customer_id'))

weather = []

for index, row in users.iterrows():
    res = (requests
            .get('https://api.openweathermap.org/data/2.5/weather?appid={key}&lon={lon}&lat={lat}&units=metric'
                .format(key = api_key,
                         lon = row['lon'],
                         lat=row['lat']))
                    .json())
    
    res["customer_id"] = row["customer_id"]

    res = (json.loads(
        re.sub(r'\[|\]', "", json.dumps(res))))
    
    weather.append(res)

weather = (pd.json_normalize(weather, sep="_"))
weather_transaction = (weather
                       .merge(sales[["customer_id", "order_id"]], on="customer_id")
                       .drop(columns="customer_id")
                       [["order_id", "weather_main", "main_temp", "main_humidity", "wind_speed"]])


## Assuming the price in the price dataset is the unit price, we can calculated the sales value as a product of the product quantity and the price.
## I will use a lambda function to create a new column with the product of the two columns.

agg_data = (final
 .assign(sale_value = lambda x: (x['price'] * x['quantity'])))

## Total sales by customer

sales_by_cus = (agg_data[["name", "customer_id", "sale_value"]]
.groupby('name')
.sum('sale_value')
.reset_index()
.rename(columns={'sale_value': 'customer_spending'}))

## Average order quantity

avg_quant = (agg_data[["quantity", "product_id"]]
 .groupby('product_id', as_index=False)
 .mean('quantity'))

## Highest revenue generating products

top_selling = (agg_data[["name", "product_id", "sale_value"]]
 .groupby('product_id')
 .sum('sale_value')
 .sort_values('sale_value', ascending=False)
 .head(10)
 .reset_index())

## Series of sales volume by month and year

monthly_volume = (agg_data[["order_date", "sale_value"]]
 .groupby(agg_data["order_date"].dt.to_period('M'))
 .sum("sale_value")
 .reset_index())

## average sale price by weather condition

avg_sale_weather = (agg_data[["customer_id", "sale_value"]]
 .merge(weather[["weather_main", "customer_id"]], on='customer_id')
 .groupby("weather_main", as_index=False)[["weather_main", "sale_value"]]
 .mean("sale_value"))

aggregates = [sales_by_cus, avg_quant, top_selling, monthly_volume, avg_sale_weather]
aggregate_tables = ['sales_by_cus', 'avg_quant', 'top_selling', 'monthly_volume', 'avg_sale_weather']

st.write("# Aggregated data dashboard")
st.write("This is a dashboard powered by `Streamlit` and `Plotly` used to demonstrated the aggregated data.")


fig1 = px.bar(sales_by_cus, x='name', y='customer_spending')
fig2 = px.bar(top_selling, 'product_id', y='sale_value')
fig2.update_xaxes(type='category')

col1, col2 = st.columns(2, gap="large")

col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True)

monthly_volume["order_date"] = monthly_volume["order_date"].astype(str)

con = sqlite3.connect("transasctions.db")
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


