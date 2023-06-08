from data_import import final, weather



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
