#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np


# In[ ]:


# TASK 1 

contracts = pd.read_csv('./contracts/Contracts.csv')

fuel_gda = pd.read_csv('./fuelPrices/GDA_TETSTX.csv')
fuel_henry = pd.read_csv('./fuelPrices/Henry Hub.csv')

plant_params = pd.read_csv('./plantParameters/Plant_Parameters.csv')

power_2016 = pd.read_csv('./powerPrices/ERCOT_DA_Prices_2016.csv')
power_2017 = pd.read_csv('./powerPrices/ERCOT_DA_Prices_2017.csv')
power_2018 = pd.read_csv('./powerPrices/ERCOT_DA_Prices_2018.csv')
power_2019 = pd.read_csv('./powerPrices/ERCOT_DA_Prices_2019.csv')


# In[ ]:


fuel_combined = pd.concat([fuel_gda, fuel_henry], axis = 0)
print(fuel_combined.to_string())


# In[ ]:


power_combined = pd.concat([power_2016, power_2017, power_2018, power_2019], axis = 0)
power_combined['Date'] = pd.to_datetime(power_combined['Date'], format="%Y-%m-%d %H:%M:%S")
print(power_combined.head(100).to_string())
print(power_combined.tail(100).to_string())


# In[ ]:


power_combined.SettlementPoint.unique() 


# In[ ]:


power_combined['year'] = power_combined.apply(lambda row: row['Date'].year, axis=1)
power_combined['month'] = power_combined.apply(lambda row: row['Date'].month, axis=1)
print(power_combined.head(10).to_string())
print(power_combined.size)


# In[ ]:


# TASK 2

grouped = power_combined.groupby(
    ['SettlementPoint', 'year', 'month']
).agg({'Price':['mean', 'min', 'max', 'std']})

print(grouped.to_string())
# NOTE: hb_pan contains only 2019 


# In[ ]:


# TASK 3 

power_combined['Pct Change'] = power_combined.groupby(
    'SettlementPoint')['Price'].pct_change(1)
power_combined = power_combined.replace([np.inf, -np.inf], np.nan)
power_combined = power_combined.dropna()


# In[ ]:


power_combined['Log Returns'] = np.log(1+power_combined['Pct Change'])


# In[ ]:


grouped = power_combined.groupby(
    ['SettlementPoint', 'year', 'month']
).agg({'Price':['mean', 'min', 'max', 'std'], 'Log Returns': ['std']})

grouped

# Volatility = Log Returns, std 


# In[ ]:


# TASK 4 
# had trouble renaming columns, log returns std is the volatility
grouped.to_csv("MonthlyPowerPriceStatistics.csv")


# In[ ]:


### 


# In[ ]:


### CONTRACT VALUATION 


# In[ ]:


### 


# In[ ]:


# TASK 5 

grouped_contracts = contracts.groupby(contracts.Granularity)
daily_contracts = grouped_contracts.get_group("Daily")
hourly_contracts = grouped_contracts.get_group("Hourly")               


# In[ ]:


daily_contracts


# In[ ]:


hourly_contracts


# In[ ]:


daily_contracts['Date'] = [pd.date_range(s, e, freq='d') for s, e in 
                           zip(pd.to_datetime(daily_contracts['StartDate']), pd.to_datetime(daily_contracts['EndDate']))]
daily_contracts = daily_contracts.explode('Date').drop(['StartDate', 'EndDate'], axis=1)
daily_contracts['year'] = daily_contracts.apply(lambda row: row['Date'].year, axis=1)
daily_contracts['month'] = daily_contracts.apply(lambda row: row['Date'].month, axis=1)
daily_contracts


# In[ ]:


# TASKS 6 & 7 

fuel_combined = pd.concat([fuel_gda, fuel_henry], axis = 0)
fuel_combined['Date'] = pd.to_datetime(fuel_combined['Date'], format="%Y-%m-%d %H:%M:%S")

fuel_combined


# In[ ]:


daily_joined = daily_contracts.set_index("Date").join(fuel_combined.set_index("Date"))
daily_joined_options = daily_joined[daily_joined['ContractName']=='O1']
daily_joined_swaps = daily_joined[daily_joined['ContractName']=='S1']

daily_joined_swaps['Payoff'] = (daily_joined_swaps['Price']-daily_joined_swaps['StrikePrice'])*daily_joined_swaps['Volume']
daily_joined_options['Payoff'] = (daily_joined_options['Price']-daily_joined_options['StrikePrice'])*daily_joined_options['Volume']
daily_joined_options['Payoff'] = daily_joined_options['Payoff'].where(daily_joined_options['Payoff'] > 0, 0)


# In[ ]:


daily_joined_options


# In[ ]:


daily_joined_swaps


# In[ ]:


power_combined = pd.concat([power_2016, power_2017, power_2018, power_2019], axis = 0)
power_combined['Date'] = pd.to_datetime(power_combined['Date'], format="%Y-%m-%d %H:%M:%S")

hourly_contracts = grouped_contracts.get_group("Hourly") 

hourly_contracts['Date'] = [pd.date_range(s, e, freq='d') for s, e in 
                           zip(pd.to_datetime(hourly_contracts['StartDate']), pd.to_datetime(hourly_contracts['EndDate']))]
hourly_contracts = hourly_contracts.explode('Date').drop(['StartDate', 'EndDate'], axis=1)

hourly_joined = hourly_contracts.set_index("Date").join(power_combined.set_index("Date"))

hourly_joined_options = hourly_joined[hourly_joined['ContractName']=='O2']
hourly_joined_swaps = hourly_joined[hourly_joined['ContractName']=='S2']

hourly_joined_swaps['Payoff'] = (hourly_joined_swaps['Price']-hourly_joined_swaps['StrikePrice'])*hourly_joined_swaps['Volume']
hourly_joined_options['Payoff'] = (hourly_joined_options['Price']-hourly_joined_options['StrikePrice'])*hourly_joined_options['Volume']
hourly_joined_options['Payoff'] = hourly_joined_options['Payoff'].where(hourly_joined_options['Payoff'] > 0, 0)
hourly_joined_options['Payoff'] = hourly_joined_options['Payoff']-(hourly_joined_options['Premium']*hourly_joined_options['Volume'])


# In[ ]:


hourly_joined_swaps


# In[ ]:


hourly_joined_options


# In[ ]:


# TASK 8 
# need to group by contract type, year & month and sum payoff column 

# TASK 9 
# take resulting dataframe from task 8 and task8_df.to_csv

