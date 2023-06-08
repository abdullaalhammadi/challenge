import requests
import pandas as pd
import os
import json
import re
from dotenv import load_dotenv

load_dotenv()

## This script is used to extract the data from sources and transform them
## Based on the business requirements. Some of the assumptions I have made are:

## 1. The quality of the data extracted from sources are guaranteed and clean (sales.csv, jsonplaceholder API, etc.)
## 2. Remote database access is not a requirement (all data fetch requests will be made via REST API's, for example)
## 3. Key definitions are not required for the database schema.

## Overall I had to make a lot of assumptions regarding business requirements, and am open for discussion
## regarding my choice of tooling and thought process.

# import random as r

api_key = os.getenv('API_KEY')
sales = pd.read_csv("../data/sales_data.csv", parse_dates=["order_date"], index_col=False)

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

## Retrieve user data from an API and normalize the JSON response into a Pandas DataFrame

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

# Convert latitude and longitude columns to float

users[["lat", "lon"]] = (users[["lat", "lon"]]
                         .astype(float))

# Merge the "sales" DataFrame with the "users" DataFrame using "customer_id" as the key

final = (sales
          .merge(users, on='customer_id'))

# Initialize an empty list to store weather data

weather = []

# Iterate over each row in the "users" DataFrame

for index, row in users.iterrows():
    # Send a request to the OpenWeatherMap API to retrieve weather data based on latitude and longitude
    res = (requests
            .get('https://api.openweathermap.org/data/2.5/weather?appid={key}&lon={lon}&lat={lat}&units=metric'
                .format(key = api_key,
                         lon = row['lon'],
                         lat=row['lat']))
                    .json())

    # Add the "customer_id" to the weather response json item
    res["customer_id"] = row["customer_id"]

    # Convert the JSON response to a string, remove square brackets (to parse nested dictionary), and convert it back to a dictionary

    res = (json.loads(
        re.sub(r'\[|\]', "", json.dumps(res))))
    
    # Append the weather data to the list

    weather.append(res)


# Normalize the weather data list into a Pandas DataFrame

weather = (pd.json_normalize(weather, sep="_"))

# Merge the weather data DataFrame with the "sales" DataFrame using "customer_id" as the key
# Then keep the order_id

weather_transaction = (weather
                       .merge(sales[["customer_id", "order_id"]], on="customer_id")
                       .drop(columns="customer_id")
                       [["order_id", "weather_main", "main_temp", "main_humidity", "wind_speed"]])