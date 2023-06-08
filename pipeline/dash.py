
import streamlit as st
import plotly.express as px
from aggregates import (sales_by_cus,
                        top_selling)

st.write("# Aggregated data dashboard")
st.write("This is a dashboard powered by `Streamlit` and `Plotly` used to demonstrated the aggregated data.")


fig1 = px.bar(sales_by_cus, x='name', y='customer_spending')
fig2 = px.bar(top_selling, 'product_id', y='sale_value')
fig2.update_xaxes(type='category')

col1, col2 = st.columns(2, gap="large")

col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True)