# Databricks notebook source
# MAGIC %pip install Flask==2.1.0 bokeh==2.4.3 geopandas databricks-sql-connector

# COMMAND ----------

import flask
from bokeh.plotting import figure
from bokeh.embed import components, file_html
from bokeh.resources import CDN
from bokeh.layouts import row, layout
from bokeh.palettes import Category20c,magma
import bokeh
import pandas as pd
from bokeh.transform import cumsum
from math import pi
from bokeh.models import Panel, Tabs, Div, GeoJSONDataSource, LinearColorMapper, HoverTool, DataTable, ColumnDataSource, TableColumn, HTMLTemplateFormatter, BasicTickFormatter, DatetimeTickFormatter
import geopandas as gpd
import random
from databricks import sql
import json

# COMMAND ----------

app = flask.Flask("Sample App")

api_url = "https://"+spark.conf.get("spark.databricks.workspaceUrl")
org_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

port = 5000

URL = f"{api_url}/driver-proxy/o/{org_id}/{cluster_id}/{str(port)}/"

# COMMAND ----------


token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

def get_data_from_query(query):
  mydb = sql.connect(server_hostname = spark.conf.get("spark.databricks.workspaceUrl"),
                  http_path       = "sql/protocolv1/o/5206439413157315/0812-164905-tear862",
                  access_token    = token)
  query = query
  result_dataFrame = pd.read_sql(query,mydb)
  return result_dataFrame

# COMMAND ----------

def create_map():

  df_map = get_data_from_query("""SELECT initcap(n_name) AS `Nation`, SUM(l_extendedprice * (1 - l_discount) * (length(n_name)/100)) AS revenue
                        FROM `samples`.`tpch`.`customer`,`samples`.`tpch`.`orders`,
                        `samples`.`tpch`.`lineitem`,`samples`.`tpch`.`supplier`,`samples`.`tpch`.`nation`,`samples`.`tpch`.`region`
                        WHERE c_custkey = o_custkey
                        AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        GROUP BY INITCAP(n_name) ORDER BY revenue DESC;""")

  my_hover = HoverTool()
  geo_path = gpd.datasets.get_path("naturalearth_lowres")
  geo_df = gpd.read_file(geo_path)
  orig = geo_df.merge(df_map, left_on = ['name'],right_on=['Nation'], how = 'inner')[['Nation','geometry','revenue']]
  p = figure(sizing_mode='stretch_both',title="Global Revenue Analysis")
  nations = df_map["Nation"].unique().tolist()
  colors = magma(len(nations))
  for i in range(0,len(nations)):
    data = orig.query(f"Nation=='{nations[i]}'")
    if len(data) == 0: continue
    geosource = GeoJSONDataSource(geojson = data.to_json())
    color_mapper = LinearColorMapper(palette = [colors[i]], nan_color = 'black')
    my_hover.tooltips = [('Revenue of the country', '@revenue{10,5}')]
    p.patches('xs','ys', source = geosource,fill_color ={ 'field':'revenue','transform':color_mapper},legend_label = nations[i])
  p.add_tools(my_hover)
  return p

# html = file_html(create_map(),CDN,'bar_chart')
# displayHTML(html)


# COMMAND ----------



# COMMAND ----------


def get_html_formatter(my_col):
    template = """
        <div style="background:<%= 
            (function colorfromint(){
                if(result_col >= 0 && result_col <= 1500000){
                    return('#dff0d8')}
                else if (result_col >= 1500001 && result_col <= 3000000)
                    {return('#fcf8e3')}
                else if (result_col >= 3000001 && result_col <= 5000000)
                    {return('#f2dede')}
                else{
                return('#f2dede')
                }
                }()) %>; 
            color: black; text-align: center;"> 
        $ <%= value.toLocaleString() %>
        </div>
    """.replace('result_col',my_col)
    
    return HTMLTemplateFormatter(template=template)

# COMMAND ----------

def create_table():

  df_div_table = get_data_from_query("""SELECT customer_id AS `Customer ID #`, total_revenue AS `TotalCustomerRevenue`
FROM ( SELECT o_custkey AS customer_id, sum(o_totalprice) as total_revenue FROM `samples`.`tpch`.`orders` GROUP BY 1 HAVING total_revenue > 0)
ORDER BY 1 LIMIT 400""")

  columns = [
      TableColumn(field='Customer ID #', title='Customer ID'),
      TableColumn(field='TotalCustomerRevenue', title='Total Customer Revenue',formatter=get_html_formatter('TotalCustomerRevenue'))
      ] 

  data_table = layout([[Div(text='<h4 style="text-align: center">Most Valued Customers</h1>')],[DataTable(source=ColumnDataSource(df_div_table),columns=columns,sizing_mode='stretch_both')]])

  return data_table

# html = file_html(create_table(),CDN,'bar_chart')
# displayHTML(html)

# COMMAND ----------

def vbar_chart():

  df_bar = get_data_from_query(""" SELECT year(o_orderdate) AS year, n_name AS nation,
   sum(l_extendedprice * (1 - l_discount) * (((length(n_name))/100) + (year(o_orderdate)-1993)/100)) AS revenue
  FROM `samples`.`tpch`.`customer`,
      `samples`.`tpch`.`orders`,`samples`.`tpch`.`lineitem`,`samples`.`tpch`.`supplier`,`samples`.`tpch`.`nation`,`samples`.`tpch`.`region`
  WHERE c_custkey = o_custkey
      AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
      AND n_name in ('ARGENTINA', 'UNITED KINGDOM', 'FRANCE','BRAZIL', 'CHINA', 'UNITED STATES', 'JAPAN', 'JORDAN') AND o_orderdate >= DATE '1994-01-01'
  GROUP BY 1,2
  ORDER BY nation ASC LIMIT 1000;""")

  countries = df_bar['nation'].unique()
  df = df_bar.pivot(index='year',columns='nation',values='revenue').reset_index()
  df['year'] = df['year'].astype(str)
  colors = magma(len(countries))
  fig = figure(x_range=df['year']
               ,title="Revenue by Nation over Time",width=1000)
  fig.vbar_stack(countries,
                 x='year',
                 width=0.5,
                 color = colors,
                 source=df,
                 legend_label = countries.tolist())
  fig.yaxis.formatter = BasicTickFormatter(use_scientific=False)
  fig.add_layout(fig.legend[0], 'right')
  return fig

# html = file_html(vbar_chart(),CDN,'bar_chart')
# displayHTML(html)

# COMMAND ----------

def create_line():
  
  df_line = get_data_from_query("""SELECT o_orderdate AS Date,o_orderpriority AS Priority,sum(o_totalprice) AS `Total Price`
                                    FROM `samples`.`tpch`.`orders` WHERE o_orderdate > '1994-01-01' AND o_orderdate < '1994-01-31'
                                    GROUP BY 1,2 ORDER BY 1, 2""")
  
  line_df = df_line.pivot(index='Date',columns='Priority',values='Total Price').reset_index()
  columns = [col for col in line_df.columns if col != 'Date']
  colors = magma(len(columns))

  p = figure(title="Shifts in Order Priorities",sizing_mode='stretch_width')
  p.yaxis.formatter = BasicTickFormatter(use_scientific=False)
  p.xaxis.formatter = DatetimeTickFormatter(days=['%Y-%m-%d'])

  for i in range(0,len(columns)):
    x = line_df['Date']
    y = line_df[columns[i]]
    p.line(x,y,line_width = 2,legend_label=columns[i],color=colors[i])
  return p


# html = file_html(create_line(),CDN,'bar_chart')
# displayHTML(html)

# COMMAND ----------

def create_layout():
  return layout([
    [create_line()],
    [create_table(), vbar_chart()]
], sizing_mode='stretch_width')

def create_tabs():
  
  panel1 = Panel(child=create_layout(), title='Tab1')
  panel2 = Panel(child=create_map(), title='Tab2')
  tabs = Tabs(tabs=[panel1, panel2])
  return tabs

# html = file_html(create_tabs(),CDN,'bar_chart')
# displayHTML(html)

# COMMAND ----------

import flask

app = flask.Flask("Sample App")

api_url = "https://"+spark.conf.get("spark.databricks.workspaceUrl")
org_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

port = 5000

URL = f"{api_url}/driver-proxy/o/{org_id}/{cluster_id}/{str(port)}/"

@app.route("/")
def show_dashboard_app():
  tabs = create_tabs()
  title = Div(text='<h1 style="text-align: center">Bokeh on Databricks</h1>', sizing_mode='stretch_both')
  top_layout = layout([[title],[tabs]], sizing_mode='stretch_width')
  
  return file_html(top_layout,CDN, "bar plot")
  

if __name__ == "__main__":
  print("-"*len(URL))
  print(URL)
  print("-"*len(URL))
  app.run(host='0.0.0.0', port=port)
