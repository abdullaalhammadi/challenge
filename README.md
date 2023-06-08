## Data Pipeline Challenge

The challenge is an ETL process of extracting data from a CSV source, joining the data with data extracted
from a REST API, aggregating data into insightful tables, dashboarding, then storing the data in a database.

### Dependencies:
- `requests`: Used to make HTTP requests and fetch data from REST API endpoints.
- `os`: Provides functions for interacting with the operating system, such as accessing environment variables and file operations.
- `pandas`: A powerful data manipulation library used for data transformations and wrangling.
- `json`: Allows working with JSON data, including parsing and serializing JSON.
- `re`: Provides support for regular expressions, used for pattern matching and substitution.
- `sqlite3`: Enables interaction with SQLite databases.
- `streamlit`: A library for creating interactive web applications.
- `plotly.express`: A library for creating interactive visualizations.
- `dotenv`: A library for reading variables from `.env` files.

## Components

### data_import.py

This code performs an Extract, Transform, Load (ETL) process on various data sources. Here's a summary of what it does:

1. Import the necessary libraries and load the API key variables from a `.env` file.
2. Retrieve sales data from a CSV file and store it in the `sales` DataFrame.
3. Retrieve user data from an API endpoint and normalize the JSON response into the `users` DataFrame.
4. Merge the `sales` DataFrame with the `users` DataFrame based on the "customer_id" column, resulting in the `final` DataFrame.
5. Make requests to the OpenWeatherMap API to fetch weather data for each user based on their latitude and longitude coordinates and store the responses in the `weather` list.
6. Normalize the `weather` list into the `weather` DataFrame.
7. Merge the `weather` DataFrame with the `sales` DataFrame based on the "customer_id" column, keeping the "order_id" column, and create the `weather_transaction` DataFrame.

### aggregates.py

This code performs various aggregations and calculations on the final and weather data to generate meaningful insights. Here's a breakdown of what it does:

1. Import the `final` and `weather` data from an external module called `data_import`.

2. Calculate the sales value by multiplying the "price" and "quantity" columns of the `final` DataFrame and create a new column called "sale_value" in the `agg_data` DataFrame using a lambda function.

3. Compute the total sales by customer by grouping the `agg_data` DataFrame by "name", summing the "sale_value" column, and renaming the resulting column to "customer_spending". The results are stored in the `sales_by_cus` DataFrame.

4. Calculate the average order quantity by grouping the `agg_data` DataFrame by "product_id" and computing the mean of the "quantity" column. The results are stored in the `avg_quant` DataFrame.

5. Determine the highest revenue-generating products by grouping the `agg_data` DataFrame by "product_id", summing the "sale_value" column, sorting in descending order, selecting the top 10 products, and storing the results in the `top_selling` DataFrame.

6. Generate a series of sales volumes by month and year by grouping the `agg_data` DataFrame by the month and year of the "order_date" column, summing the "sale_value" column, and storing the results in the `monthly_volume` DataFrame.

7. Calculate the average sale price by weather condition by merging the `agg_data` DataFrame with the `weather` DataFrame on the "customer_id" column, grouping by "weather_main", and computing the mean of the "sale_value" column. The results are stored in the `avg_sale_weather` DataFrame.

8. Create a list called `aggregates` that contains the resulting aggregated DataFrames: `sales_by_cus`, `avg_quant`, `top_selling`, `monthly_volume`, and `avg_sale_weather`.

9. Create a list called `aggregate_tables` that contains the table names corresponding to the aggregated DataFrames.

These aggregations and calculations provide insights into customer spending, average order quantity, top-selling products, sales volume over time, and average sale price by weather condition.


### dash.py


This code creates an aggregated data dashboard using Streamlit and Plotly. It displays two interactive bar charts: one showing total customer spending and another showing the top-selling products. The charts provide insights into customer behavior and sales performance, allowing users to explore the aggregated data visually.

1. Import the necessary libraries: `streamlit` for creating interactive web applications and `plotly.express` for creating interactive visualizations.

2. Create a Streamlit application and define the title and description of the aggregated data dashboard.

3. Create two bar charts (`fig1` and `fig2`) using Plotly Express. 
   - `fig1` represents the total customer spending, with "name" on the x-axis and "customer_spending" on the y-axis.
   - `fig2` represents the top-selling products, with "product_id" on the x-axis and "sale_value" on the y-axis.

4. Update the x-axis of `fig2` to display the "product_id" as a category.

5. Divide the Streamlit application into two columns (`col1` and `col2`) using `st.columns()`, with a large gap between them.

6. Display `fig1` in the first column (`col1`) using `col1.plotly_chart()`, and set `use_container_width=True` to make the chart responsive to the column width.

7. Display `fig2` in the second column (`col2`) using `col2.plotly_chart()`, and also set `use_container_width=True` for responsiveness.

## load.py

This code sets up a SQLite database and creates multiple tables to store the data. It then inserts the relevant DataFrames into their respective tables in the database. This allows for efficient data storage and retrieval for future analysis and reporting.

1. Import the necessary libraries: `pandas` for data manipulation and `sqlite3` for working with SQLite databases.

2. Import the `aggregates`, `aggregate_tables`, and `monthly_volume` data from an external module called `aggregates`, and import the `final`, `weather_transaction`, `users`, and `sales` data from an external module called `data_import`.

3. Convert the "order_date" column of the `monthly_volume` DataFrame to a string data type.

4. Establish a connection to the SQLite database located at "../transactions.db" and create a cursor object.

5. Execute SQL statements to create four tables in the database: "sales_customers", "sales", "weather", and "customers". Each table has a specific set of columns defined.

6. Store the `final`, `weather_transaction`, `users`, and `sales` DataFrames in their respective tables in the SQLite database using the `to_sql()` method. Existing data is appended to the tables, and the index column is excluded.

7. Create a new table called "products" by extracting the unique values from the "product_id" column of the `sales` DataFrame and replacing the existing "products" table in the database.

8. Iterate through the `aggregates` list and store each DataFrame in its corresponding table in the database using the `to_sql()` method. The `aggregate_tables` list is used to determine the table names.

9. Close the database cursor.

## Diagram

[Diagram](mermaid-diagram-2023-06-07-214722.svg)
