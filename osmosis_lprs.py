"""
Created on Tue Oct 25 23:36:54 2022
@author: Jordi Garcia Ruspira
"""

import streamlit as st
import pandas as pd
import requests
import json
import time
import plotly.graph_objects as go
import random
import plotly.io as pio 
import plotly.express as px
import plotly.graph_objects as go
import datetime as dt 
import matplotlib.pyplot as plt
import numpy as np
from plotly.subplots import make_subplots
from PIL import Image 
import datetime 
import plotly.graph_objs as go

import plotly.graph_objects as go

import networkx as nx

st.set_page_config(
        page_title="Osmosis - Liquidity whales ",
        page_icon=":atom_symbol:",
        layout="wide",
        menu_items=dict(About="It's a work of Jordi"),
)
    
     
     
im_col1, im_col2, im_col3 = st.columns(3) 
im_col1.image(
        "https://i.ibb.co/61KHMYc/im1.png" 
    )

im_col2.image(
       "https://i.ibb.co/xFKcYC6/im2.png" 
    )
    
    
im_col3.image(
       "https://i.ibb.co/nDyJdb7/DALL-E-2022-10-25-22-34-07.png" 
) 

 
pio.renderers.default = 'browser'
    
    
    
    
API_KEY = st.secrets["API_KEY"]


    
SQL_QUERY_0 = """  with all_lp_action as (select pool_id[0] as pool_id, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
            where TX_SUCCEEDED = 'TRUE'
            and action in ('lp_tokens_minted','lp_tokens_burned')
            group by pool_id, action, currency)
            
            select pool_id, sum(amount_translated_2) as total_gamm_pool from all_lp_action
            group by pool_id
            
                """  
SQL_QUERY_1 = """  with all_lp_action_address as (
            select pool_id[0] as pool_id, liquidity_provider_address, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
            where TX_SUCCEEDED = 'TRUE'
            and action in ('lp_tokens_minted','lp_tokens_burned')
            group by pool_id,liquidity_provider_address, action, currency)
            
            select pool_id,liquidity_provider_address, sum(amount_translated_2) as total_shares_lp_address from all_lp_action_address 
            group by pool_id,liquidity_provider_address
            
                """  
SQL_QUERY_2_AUX = """ with all_lp_action_address as (
            select pool_id[0] as pool_id, liquidity_provider_address, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
            where TX_SUCCEEDED = 'TRUE'
            and action in ('lp_tokens_minted','lp_tokens_burned')
            group by pool_id,liquidity_provider_address, action, currency),
            
            
            all_lp_action as (select pool_id[0] as pool_id, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
            where TX_SUCCEEDED = 'TRUE'
            and action in ('lp_tokens_minted','lp_tokens_burned')
            group by pool_id, action, currency),
            
            all_lp_amount as (          
            select pool_id, sum(amount_translated_2) as total_gamm_pool from all_lp_action
            group by pool_id),
            
            final_table as (            
            select pool_id,liquidity_provider_address, sum(amount_translated_2) as total_shares_lp_address from all_lp_action_address a 
            where liquidity_provider_address = '"""  
    
    
    
SQL_QUERY_3 = """ with all_lp_action_address as (
select pool_id[0] as pool_id, liquidity_provider_address, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
where TX_SUCCEEDED = 'TRUE'
and action in ('lp_tokens_minted','lp_tokens_burned')
group by pool_id,liquidity_provider_address, action, currency),
all_lp_action as (select pool_id[0] as pool_id, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
where TX_SUCCEEDED = 'TRUE'
and action in ('lp_tokens_minted','lp_tokens_burned')
group by pool_id, action, currency),
all_lp_amount as (          
select pool_id, sum(amount_translated_2) as total_gamm_pool from all_lp_action
group by pool_id),
final_table as (            
select pool_id,liquidity_provider_address, sum(amount_translated_2) as total_shares_lp_address from all_lp_action_address a 
group by pool_id,liquidity_provider_address),
last_final_table as (
select a.pool_id, liquidity_provider_address, total_shares_lp_address/total_gamm_pool as percentage_over_pool, row_number() over (partition by a.pool_id order by percentage_over_pool desc)  as rank from final_table a 
left join all_lp_amount b 
on a.pool_id = b.pool_id
--where a.pool_id in ('1','678')
where a.pool_id not in (select distinct pool_id from all_lp_amount where total_gamm_pool = 0)
order by rank desc )
select * from last_final_table
where rank <= 30
            
                """  
                
#The following query returns, for each pool, the % of the pool the top 30 LPrs have           
SQL_QUERY_4 = """
    with all_lp_action_address as (
select  pool_id[0] as pool_id, liquidity_provider_address, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
where TX_SUCCEEDED = 'TRUE'
and action in ('lp_tokens_minted','lp_tokens_burned')
group by pool_id,liquidity_provider_address, action, currency),
all_lp_action as (select  pool_id[0] as pool_id, action, currency, sum(amount/pow(10, decimal)) as amount_translated, case when action = 'lp_tokens_minted' then amount_translated else amount_translated*(-1) end as amount_translated_2 from osmosis.core.fact_liquidity_provider_actions
where TX_SUCCEEDED = 'TRUE'
and action in ('lp_tokens_minted','lp_tokens_burned')
group by pool_id, action, currency),
all_lp_amount as (          
select pool_id, sum(amount_translated_2) as total_gamm_pool from all_lp_action
group by pool_id),
final_table as (            
select pool_id,liquidity_provider_address, sum(amount_translated_2) as total_shares_lp_address from all_lp_action_address a 
group by pool_id,liquidity_provider_address),
  
last_final_table as (
select a.pool_id, liquidity_provider_address, total_shares_lp_address/total_gamm_pool as percentage_over_pool, row_number() over (partition by a.pool_id order by percentage_over_pool desc)  as rank from final_table a 
left join all_lp_amount b 
on a.pool_id = b.pool_id 
where a.pool_id not in (select distinct pool_id from all_lp_amount where total_gamm_pool = 0)
order by rank desc ),
last_final_table_2 as (
select * from last_final_table
where rank <= 30)
select pool_id, sum(percentage_over_pool)*100 as top_30_holders_percentage from last_final_table_2
group by pool_id
    
    """
TTL_MINUTES = 15
# return up to 100,000 results per GET request on the query id
PAGE_SIZE = 100000
# return results of page 1
PAGE_NUMBER = 1
    
def create_query(SQL_QUERY):
    r = requests.post(
            'https://node-api.flipsidecrypto.com/queries', 
            data=json.dumps({
                "sql": SQL_QUERY,
                "ttlMinutes": TTL_MINUTES
            }),
            headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
    )
    if r.status_code != 200:
        raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
        
    return json.loads(r.text)    
       
    
def get_query_results(token):
    r = requests.get(
            'https://node-api.flipsidecrypto.com/queries/{token}?pageNumber={page_number}&pageSize={page_size}'.format(
              token=token,
              page_number=PAGE_NUMBER,
              page_size=PAGE_SIZE
            ),
            headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
    )
    if r.status_code != 200:
        raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
        
    data = json.loads(r.text)
    if data['status'] == 'running':
        time.sleep(10)
        return get_query_results(token)
    
    return data
    
query = create_query(SQL_QUERY_0)
token = query.get('token')
data0 = get_query_results(token) 
df0 = pd.DataFrame(data0['results'], columns = ['POOL_ID', 'TOTAL_GAMM_POOL'])  
    
query = create_query(SQL_QUERY_1)
token = query.get('token')
data1 = get_query_results(token) 
df1 = pd.DataFrame(data1['results'], columns = ['POOL_ID', 'LIQUIDITY_PROVIDER_ADDRESS','TOTAL_GAMM_POOL'])  
    
    
    

    
#This part here is to get a dictionary with pool_id and the pair. 
pairs_summary = 'https://api-osmosis.imperator.co/pairs/v1/summary'
pairs_summary_json = requests.get(pairs_summary).json()
pairs_summary_df = pd.DataFrame(pairs_summary_json)
pairs_summary_data = pairs_summary_df['data'] 
pairs = []
        
for i in range(0, len(pairs_summary_data),1):
            
    d = {
              'POOL_ID': pairs_summary_data[i]['pool_id'],
              'pair':pairs_summary_data[i]['base_symbol']+"-"+pairs_summary_data[i]['quote_symbol'],
              'volume_api':"https://api-osmosis.imperator.co/pools/v2/volume/"+pairs_summary_data[i]['pool_id']+"/chart",
              'liquidity_api':"https://api-osmosis.imperator.co/pools/v2/"+pairs_summary_data[i]['pool_id']

    } 
            
    pairs.append(d)
        
pairs = pd.DataFrame(pairs)

new_rows = pd.DataFrame({'POOL_ID': [633, 818], 'pair': ['USDC.grv-OSMO', 'USDT.grv-OSMO'], 'volume_api': ['https://api-osmosis.imperator.co/pools/v2/volume/633/chart', 'https://api-osmosis.imperator.co/pools/v2/volume/818/chart'], 'liquidity_api': ['https://api-osmosis.imperator.co/pools/v2/633', 'https://api-osmosis.imperator.co/pools/v2/818']})

# concatenate the new dataframe with the original dataframe
pairs = pd.concat([pairs, new_rows], ignore_index=True)     
   
query = create_query(SQL_QUERY_3)
token = query.get('token')
data3 = get_query_results(token) 
df3 = pd.DataFrame(data3['results'], columns = ['POOL_ID', 'LIQUIDITY_PROVIDER_ADDRESS','PERCENTAGE_OVER_POOL','RANK'])  
     
 

test_json = requests.get('https://api-osmosis.imperator.co/pools/v2/all?low_liquidity=true').json()
test_json_keys = test_json.keys()

df_test_aux = []
test_df = []
for i in test_json_keys:
    df_test_aux = pd.DataFrame(test_json[i])
    df_test_aux['POOL_ID'] = int(i)
    test_df.append(df_test_aux) 

    
df_all_pools = pd.concat(test_df, ignore_index=True)

       
Inner_join_2 = pd.merge(df_all_pools[['symbol','POOL_ID','amount']], 
                         df3, 
                         on ='POOL_ID', 
                         how ='inner')
    
Inner_join_2['amount_token'] = Inner_join_2['PERCENTAGE_OVER_POOL']*Inner_join_2['amount']
    
     
    
query = create_query(SQL_QUERY_4)
token = query.get('token')
data4 = get_query_results(token) 
df4 = pd.DataFrame(data4['results'], columns = ['POOL_ID', 'TOP_30_HOLDERS_PERCENTAGE'])  
      
#Default values
pool_choice = 'ATOM-OSMO'
pool_choice_id = 1
    

st.title("Osmosis liquidity - a deep dive user interactive dashboard")
st.text("")
st.success("This app serves as an addition from [this Flipside dashboard](https://app.flipsidecrypto.com/dashboard/osmosis-liquidity-providers-is-liquidity-concentrated-yGbpMf). For further context and explanation, please refer to that link!")
st.text("")
st.subheader('Dashboard by [Jordi R](https://twitter.com/RuspiTorpi/). Powered by Flipsidecrypto & Imperator')
st.text("")
st.header("1. Introduction")   
st.text("")
st.markdown("Osmosis is home to the leading DEX appchain on Cosmos and the most active zone, as the go-to place for users looking for a window to the rest of app-chains. So how is liquidity distributed amongst the pools? Is it concentrated within a few whales, are whales in a pool also big contributors on other pools? This dashboard pretends to be a tool to explore precisely this information.")
st.text("") 
st.header("2. Liquidity concentration")
st.text("") 
st.subheader("2.1. Gauge indicator as a tool")
st.text("")
st.markdown("It's not a secret that a big flaw on most DEXs in the blockchain ecosystem is the constant lookout for bigger returns by users. Once a new DEX with higher returns appears, there's always a big move in liquidity from other platforms to the new one. This might be even a bigger issue for DEXs which have liquidity concentrated within a few users. On one hand, it might be easier for them to move outside the platform once higher return pools appear, but since they'd hold much liquidity, it would also mean that they cannot exit a pool without having to pay a high slippage, so it's a double edged sword in this case. ")
st.text("")
st.markdown("I've created a few tools to help regular users understand and visualize how concetrated liquidity is in a pool of their choice.")


commentaryCol, chartCol = st.columns((3,6))

pairs.loc[pairs['POOL_ID'] == '877', 'pair'] = 'BUSD-USDC-USDT'
pairs.loc[pairs['POOL_ID'] == '679', 'pair'] = 'FRAX-USDT-USTC-USDC'
pairs['pair'] = pairs.apply(lambda x: str(x['POOL_ID']) + "-" + x['pair'], axis=1)
pairs = pairs.drop_duplicates()
pool_choice = commentaryCol.selectbox("Select a pool", options = pairs['pair'].unique() )
pool_choice_id = pairs['POOL_ID'][pairs['pair'] == str(pool_choice)].to_string(index=False)
commentaryCol.markdown("By selecting a pool of your choice in the selection box above, the chart on the right hand side will change accordingly.")
commentaryCol.markdown("")
commentaryCol.markdown("More specifically, the chart will show how much of the total liquidity of the selected pool is currently held by the top 30 liquidity providers of said pool.")

#Values change once selected
    
top30_percentage = df4['TOP_30_HOLDERS_PERCENTAGE'][df4['POOL_ID'] == int(pool_choice_id)]

    
     

fig = go.Figure(go.Indicator(
        mode = "number+gauge",
        value = float(top30_percentage.to_string(index=False)),
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Amount of liquidity provided (in % of the total pool) by the top 30 wallets"},
        gauge = {
        'shape': "angular",
        'axis': {'range': [0, 100]}}))
     
chartCol.plotly_chart(fig, use_container_width=True) 
     

df3_filtered = df3[['POOL_ID','LIQUIDITY_PROVIDER_ADDRESS','PERCENTAGE_OVER_POOL','RANK']][df3['POOL_ID'] == int(pool_choice_id)]
df3_filtered['PERCENTAGE_OVER_POOL'] = df3_filtered['PERCENTAGE_OVER_POOL']*100
df3_filtered = df3_filtered.sort_values(by ='RANK', ascending = True)

  
chartCol2, commentaryCol2 = st.columns((5,4))


 
fig = px.bar(df3_filtered, x='LIQUIDITY_PROVIDER_ADDRESS', y='PERCENTAGE_OVER_POOL', title="Address and its liquidity percentage of all the selected pool",
        height=1000)
chartCol2.plotly_chart(fig, use_container_width=True) 

commentaryCol2.text("")
commentaryCol2.text("")
commentaryCol2.text("")
commentaryCol2.markdown("Meanwhile, the chart on the left hand side gives more insight on the individual wallets that add up for the total percentage displayed above. Showing the amount in percentage held by each user from the top 30 liquidity providers shows even more info on another layer of liquidity concentration.")
commentaryCol2.text("")
commentaryCol2.markdown("There might be one or a few addresses that stand out from the rest, or they might all hold similar amounts.")
commentaryCol2.text("")

Inner_join_2_filtered = Inner_join_2[['symbol','POOL_ID','LIQUIDITY_PROVIDER_ADDRESS','PERCENTAGE_OVER_POOL','amount_token','RANK']][Inner_join_2['POOL_ID'] == int(pool_choice_id)]
Inner_join_2_filtered = Inner_join_2_filtered.sort_values(by ='RANK', ascending = True)
Inner_join_2_filtered['PERCENTAGE_OVER_POOL'] = Inner_join_2_filtered['PERCENTAGE_OVER_POOL']*100

st.success("The following table shows the top 30 holders of the selected pool information; amount of tokens held, rank and amount in USD.")



st.table(Inner_join_2_filtered)

def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')


csv = convert_df(Inner_join_2_filtered)

st.download_button(
   "Press to Download",
   csv,
   "file.csv",
   "text/csv",
   key='download-csv'
)
st.write("")
st.subheader('Total holdings from the top 30 wallets of all pools')
st.write("")
st.write("Currently, if we sum all the liquidity currently being provided by the top 30 liquidity providers of each pool, we get more than 100M USD. Given that there is actually 210M USD in liquidity on Osmosis, it's save to say that almost half of all the liquidity on Osmosis is provided by the top 30 individuals of all pools.")
total_holdings = pd.merge(Inner_join_2, df_all_pools[['symbol','POOL_ID','amount','price']],  how='left', left_on=['POOL_ID','symbol'], right_on = ['POOL_ID','symbol'])
total_holdings['usd'] = total_holdings['amount_token']*total_holdings['price']



st.metric(label="Total LP holdings (in USD) in all the pools, for the top 30 liquidity providers on each pool", value= round(total_holdings['usd'].sum()))

st.write("")
st.success("I've added a feature where you can introduce a wallet, and it will show the pool holdings (not staked or other assets).")
st.write("")



 
input_feature = st.text_input('Introduce wallet address: ','osmo1s3uhtyzcu2ft4w2dhjtew3gt3lpmc2az2rw5ll')
    
# Here we create a sql query which returns the gamm amount for each pool that the wallet holds on osmosis, in percentage
    
SQL_QUERY_2 = SQL_QUERY_2_AUX+ input_feature + "' group by pool_id,liquidity_provider_address) select a.pool_id, liquidity_provider_address, total_shares_lp_address/total_gamm_pool as percentage_over_pool from final_table a  left join all_lp_amount b  on a.pool_id = b.pool_id  " 
    
  
query = create_query(SQL_QUERY_2)
token = query.get('token')
data2 = get_query_results(token) 
df2 = pd.DataFrame(data2['results'], columns = ['POOL_ID', 'LIQUIDITY_PROVIDER_ADDRESS','PERCENTAGE_OVER_POOL'])  
   

Inner_join = pd.merge(df_all_pools[['symbol','POOL_ID','amount','price']], 
                         df2, 
                         on ='POOL_ID', 
                      how ='inner')   

Inner_join['amount_token'] = Inner_join['amount']*Inner_join['PERCENTAGE_OVER_POOL']
Inner_join['amount_usd'] = Inner_join['amount_token']*Inner_join['price']
Inner_join = Inner_join[Inner_join['amount_token'] > 0]
Inner_join['PERCENTAGE_OVER_POOL'] = pd.to_numeric(Inner_join['PERCENTAGE_OVER_POOL'])
Inner_join_filtered = Inner_join[['POOL_ID','symbol','LIQUIDITY_PROVIDER_ADDRESS','PERCENTAGE_OVER_POOL','amount_token','amount_usd']]
 

st.table(Inner_join_filtered)


st.write("")
st.write("The selected address holds assets on the following pools. You can now select one of them, and see the amount it has in USD on that pool!")
st.write("")

pool_choice = st.selectbox("Select a pool", options = Inner_join_filtered['POOL_ID'].unique() )

col1, col2 = st.columns(2)

col1.metric(label="Total amount (USD) of all pool holdings for selected wallet", value=Inner_join_filtered['amount_usd'].sum())

col2.metric(label="Amount (USD) of pool holdings for selected wallet and selected pool", value=Inner_join_filtered['amount_usd'][Inner_join_filtered['POOL_ID']== pool_choice].sum())





st.write("")
st.success("Finally, we can compare top liquidity providers between pools.")
st.write("") 
st.write("The following chart displays, from the top 30 liquidity providers in the selected pool at the start of the dashboard, their aggregated percentage over the rest of the pools.")
st.write("")





new_df = df3[['LIQUIDITY_PROVIDER_ADDRESS','POOL_ID','PERCENTAGE_OVER_POOL']][(df3['LIQUIDITY_PROVIDER_ADDRESS'].isin(df3['LIQUIDITY_PROVIDER_ADDRESS'][df3['POOL_ID'] == int(pool_choice_id)])) &  (df3['POOL_ID'] != int(pool_choice_id))]
new_df['PERCENTAGE_OVER_POOL'] = new_df['PERCENTAGE_OVER_POOL']*100

 

new_df_grouped = new_df.groupby(by=["POOL_ID"]).sum().reset_index(drop=False)
pairs['POOL_ID'] = pairs['POOL_ID'].astype(str) 
new_df_grouped['POOL_ID'] = new_df_grouped['POOL_ID'].astype(str)
new_df_grouped = pd.merge(new_df_grouped[['POOL_ID','PERCENTAGE_OVER_POOL']], 
                         pairs, 
                         on ='POOL_ID', 
                      how ='inner') 


 
 
new_df_grouped = new_df_grouped.sort_values(by ='PERCENTAGE_OVER_POOL', ascending = False)
fig = px.bar(new_df_grouped, x='pair', y='PERCENTAGE_OVER_POOL')
 
st.plotly_chart(fig, use_container_width=True) 
 


st.write("")
st.write("Since the first select box is way up in the dashboard, the following two columns perform the same action. You can choose a pool, and the corresponding chart below will refresh with the following information: ")
st.markdown("* It takes the top 30 liquidity providers for the selected pool")
st.markdown("* It looks at the other pools, how much liquidity those previous liquidity providers have in the rest of the pools")
st.markdown("* It aggregates by sum of liquidity and pool, therefore showing relationship between the selected pool and the rest")


st.write("") 

pool_choice_id_new_1 = 1  
pool_choice_id_new_1 = 22
col1, col2 = st.columns(2) 
pool_choice_new_1 = col1.selectbox("Select a first pool", options = pairs['pair'].unique() )
pool_choice_new_2 = col2.selectbox("Select another pool", options = pairs['pair'].unique() )
pool_choice_id_new_1 = pairs['POOL_ID'][pairs['pair'] == str(pool_choice_new_1)].to_string(index=False)
pool_choice_id_new_2 = pairs['POOL_ID'][pairs['pair'] == str(pool_choice_new_2)].to_string(index=False)



new_df_1 = df3[['LIQUIDITY_PROVIDER_ADDRESS','POOL_ID','PERCENTAGE_OVER_POOL']][(df3['LIQUIDITY_PROVIDER_ADDRESS'].isin(df3['LIQUIDITY_PROVIDER_ADDRESS'][df3['POOL_ID'] == int(pool_choice_id_new_1)])) &  (df3['POOL_ID'] != int(pool_choice_id_new_1))]
new_df_1['PERCENTAGE_OVER_POOL'] = new_df_1['PERCENTAGE_OVER_POOL']*100
new_df_2 = df3[['LIQUIDITY_PROVIDER_ADDRESS','POOL_ID','PERCENTAGE_OVER_POOL']][(df3['LIQUIDITY_PROVIDER_ADDRESS'].isin(df3['LIQUIDITY_PROVIDER_ADDRESS'][df3['POOL_ID'] == int(pool_choice_id_new_2)])) &  (df3['POOL_ID'] != int(pool_choice_id_new_2))]
new_df_2['PERCENTAGE_OVER_POOL'] = new_df_2['PERCENTAGE_OVER_POOL']*100
 

new_df_grouped_1 = new_df_1.groupby(by=["POOL_ID"]).sum().reset_index(drop=False) 
new_df_grouped_1['POOL_ID'] = new_df_grouped_1['POOL_ID'].astype(str)
new_df_grouped_1 = pd.merge(new_df_grouped_1[['POOL_ID','PERCENTAGE_OVER_POOL']], 
                         pairs, 
                         on ='POOL_ID', 
                      how ='inner') 

new_df_grouped_1 = new_df_grouped_1.sort_values(by ='PERCENTAGE_OVER_POOL', ascending = False)
fig = px.bar(new_df_grouped_1, x='pair', y='PERCENTAGE_OVER_POOL', hover_data = ['POOL_ID'])
 
col1.plotly_chart(fig, use_container_width=True) 

new_df_grouped_2 = new_df_2.groupby(by=["POOL_ID"]).sum().reset_index(drop=False) 
new_df_grouped_2['POOL_ID'] = new_df_grouped_2['POOL_ID'].astype(str)
new_df_grouped_2 = pd.merge(new_df_grouped_2[['POOL_ID','PERCENTAGE_OVER_POOL']], 
                         pairs, 
                         on ='POOL_ID', 
                      how ='inner') 

new_df_grouped_2 = new_df_grouped_2.sort_values(by ='PERCENTAGE_OVER_POOL', ascending = False)
fig = px.bar(new_df_grouped_2, x='pair', y='PERCENTAGE_OVER_POOL', hover_data =['POOL_ID']) 
col2.plotly_chart(fig, use_container_width=True) 
