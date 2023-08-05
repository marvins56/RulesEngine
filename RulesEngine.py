import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import base64
import plotly.graph_objs as go
import numpy as np
import random

# Load Data Function
def load_data(uploaded_file):
    if uploaded_file.type == "text/csv":
        data = pd.read_csv(uploaded_file, thousands=',')
    elif uploaded_file.type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.ms-excel"]:
        data = pd.read_excel(uploaded_file)
    else:
        st.error("Unsupported file type")
        return None

    # Strip unnecessary spaces from column names
    data.columns = data.columns.str.strip()
    return data

# Data Cleaning Function
def clean_data(df):
    # Notify beginning of data cleansing
    st.write("Starting data cleansing...")
    
    # Replace missing values with appropriate default values based on data type
    default_values = {
        'Date': pd.to_datetime('2000-01-01'), # Replace missing dates with a default date (e.g., '1900-01-01')
        'Agent Account': 'Unknown', # Replace missing agent accounts with 'Unknown'
        'Customer': 'Unknown', # Replace missing customer names with 'Unknown'
        'Item': 'Unknown', # Replace missing item names with 'Unknown'
        'Amount': 0, # Replace missing amounts with 0
    }
    df.fillna(default_values, inplace=True)
    
    # Convert 'Date' column to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Convert 'Amount' column to numeric format
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    
    # Remove leading/trailing whitespaces from column names
    df.columns = df.columns.str.strip()
    
    st.write("Data cleansing completed!")
    return df

# Function to categorize transactions by agent
def categorize_by_agent(data):
    agent_categories = {}
    for agent in data['Agent Account'].unique():
        agent_data = data[data['Agent Account'] == agent]
        agent_categories[agent] = agent_data
    return agent_categories

# Function to create download buttons for each agent category
def download_categories(agent_categories):
    st.subheader("Download Agent Categories")

    for agent, agent_data in agent_categories.items():
        csv_data = agent_data.to_csv(index=False)
        b64 = base64.b64encode(csv_data.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{agent}_category.csv">Download {agent} Category</a>'
        st.markdown(href, unsafe_allow_html=True)


def color_by_agent(data, agent_column='Agent Account'):
    # Create a dictionary to map each agent to a random color
    unique_agents = data[agent_column].unique()
    agent_colors = {agent: f'#{random.randint(0, 0xFFFFFF):06X}' for agent in unique_agents}

    # Define a function to apply the color to rows based on the agent
    def apply_color(row):
        agent = row[agent_column]
        color = agent_colors.get(agent, 'white')
        return [f'background-color: {color}']*len(row)

    # Apply the coloring function to the DataFrame
    return data.style.apply(apply_color, axis=1)

# Function to render flagged transactions table with random colors
def render_flagged_transactions_InTable(agent_data, flagged_transactions):
    st.subheader(f"Flagged Transactions for Agent {agent_data['Name'].iloc[0]}")

    # Generate a random color for the agent
    agent_color = f'#{random.randint(0, 0xFFFFFF):06x}'

    # Apply styling to the flagged_transactions DataFrame
    flagged_transactions_table = flagged_transactions.style.apply(
        lambda x: f'background: {agent_color}' if x.name in flagged_transactions.index else '', axis=1
    )

    # Render the table with flagged transactions highlighted in the agent's color
    st.dataframe(flagged_transactions_table)

def rule_liquidations_same_number(data, time_window, number_column, biller_column, item_column, timestamp_column):
    flagged_transactions = []

    # Filter data for Mobile Money Liquidations with Airtel Float or MTN Float
    data_filtered = data[
        (data[biller_column] == "Mobile Money Liquidation") &
        ((data[item_column] == "Airtel Float Liquidation") | (data[item_column] == "MTN Float Liquidation"))
    ]

    # Sort data by timestamp
    data_sorted = data_filtered.sort_values(by=timestamp_column)

    # Iterate through rows to find flagged transactions
    for index, transaction in data_sorted.iterrows():
        mask = (data_sorted[number_column] == transaction[number_column]) & \
               (data_sorted[timestamp_column] >= transaction[timestamp_column] - timedelta(minutes=time_window)) & \
               (data_sorted[timestamp_column] < transaction[timestamp_column])

        # Check if there are any previous transactions within the time window
        if data_sorted[mask].shape[0] > 0:
            flagged_transactions.append(transaction)

    flagged_transactions = pd.DataFrame(flagged_transactions)

    return flagged_transactions



# Rule 1: Deposits (bank deposits) or Float purchases (MTN or Airtel float purchases) 
# to the same account by the same agent within a specified time window (minutes)

def rule_deposit_or_float_same_account_time_window(data, time_window, agent_column, account_column, date_column):
    # Filter transactions for bank deposits and float purchases based on keywords
    bank_deposit_keywords = ['deposit', 'deposits', 'bank']
    mtn_float_keywords = ['MTN Float', 'MTN Float purchase']
    airtel_float_keywords = ['Airtel Float', 'Airtel Float purchase']

    bank_deposits = data[data[account_column].str.contains('|'.join(bank_deposit_keywords), case=False)]
    mtn_float_purchases = data[data[account_column].str.contains('|'.join(mtn_float_keywords), case=False)]
    airtel_float_purchases = data[data[account_column].str.contains('|'.join(airtel_float_keywords), case=False)]

    # Combine the filtered transactions for bank deposits and float purchases
    filtered_data = pd.concat([bank_deposits, mtn_float_purchases, airtel_float_purchases])

    # Sort the data by the date column in ascending order
    data_sorted = filtered_data.sort_values(by=date_column)

    # Initialize an empty list to store flagged transactions
    flagged_transactions = []

    # Iterate through each transaction in the sorted data
    for index, transaction in data_sorted.iterrows():
        # Find transactions made by the same agent to the same account within the time window
        mask = (data_sorted[agent_column] == transaction[agent_column]) & \
               (data_sorted[account_column] == transaction[account_column]) & \
               (data_sorted[date_column] >= transaction[date_column]) & \
               (data_sorted[date_column] <= transaction[date_column] + pd.Timedelta(minutes=time_window))

        # Check if there are any other transactions within the time window
        if len(data_sorted[mask]) > 1:
            flagged_transactions.extend(data_sorted[mask].to_dict(orient='records'))

    # Convert the list of flagged transactions to a DataFrame
    flagged_transactions = pd.DataFrame(flagged_transactions)

    return flagged_transactions


# Main Function
def RulesEngine():
    st.title("Mobile Money & Agent Banking Rules Engine")

    # Upload CSV or Excel file
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])
    if uploaded_file is not None:
        data = load_data(uploaded_file)

        # Clean data
        data = clean_data(data)

        # Data overview
        st.subheader("Data Overview")
        st.write(data.head())

        # Data processing and visualization for rules
        if len(data) > 0:
            # Rule Selection Dropdown
            selected_rule = st.selectbox("Select a rule to inspect transactions:", 
                                         ["---", 
                                          "Rule: Liquidations from the same number within a specified time window (minutes)",
                                          "Rule: Deposits or Float purchases to the same account within a specified time window (minutes)"
                                          # Add more rule options here...
                                          ])
            if selected_rule != "---":
                # Apply the selected rule function and display the resulting transactions
                if selected_rule == "Rule: Liquidations from the same number within a specified time window (minutes)":
                    time_window = st.slider("Select the time window for liquidations (in minutes)", min_value=1, max_value=60, value=10)
                    flagged_transactions = rule_liquidations_same_number(data, time_window, 'Customer', 'Biller', 'Item', 'Date')

                    # Display flagged transactions
                    st.subheader("Flagged Transactions")
                    st.dataframe(color_by_agent(flagged_transactions))

                elif selected_rule == "Rule: Deposits or Float purchases to the same account within a specified time window (minutes)":
                    time_window = st.slider("Select the time window for deposits/float purchases (in minutes)", min_value=1, max_value=60, value=10)
                    # flagged_transactions = rule_deposit_or_float_same_account_time_window(data, time_window, 'Agent Account', 'Biller', 'Item', 'Date')
                    flagged_transactions = rule_deposit_or_float_same_account_time_window(data, time_window, 'Agent Account', 'Biller', 'Date')

                    

                    # Display flagged transactions
                    st.subheader("Flagged Transactions")
                    st.dataframe(color_by_agent(flagged_transactions))

                # Add more rule options here...
                
               
            else:
                st.info("Please select a rule to inspect transactions.")
        else:
            st.warning("Uploaded file is empty. Please upload a valid CSV or Excel file.")

if __name__ == "__main__":
    RulesEngine()



# def RulesEngine():
#     def color_by_agent(data, agent_column='Agent Account'):
#         unique_agents = data[agent_column].unique()
#         agent_colors = {agent: f'#{random.randint(0, 0xFFFFFF):06X}' for agent in unique_agents}
#         def apply_color(row):
#             agent = row[agent_column]
#             color = agent_colors.get(agent, 'white')
#             return [f'background-color: {color}']*len(row)
#         return data.style.apply(apply_color, axis=1)

#     st.title("Mobile Money & Agent Banking Rules Engine")

#     # Upload CSV or Excel file
#     uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])
#     if uploaded_file is not None:
#         data = load_data(uploaded_file)

#         # Clean data
#         data = clean_data(data)

#         # Data overview
#         st.subheader("Data Overview")
#         st.dataframe(color_by_agent(data.head()))

#         # Rule Selection Dropdown
#         selected_rule = st.selectbox("Select a rule to inspect transactions:", 
#                                      ["---", 
#                                       "Rule: Liquidations from the same number within a specified time window (minutes)",
#                                       "Rule: Deposits or Float purchases to the same account within a specified time window (minutes)"
#                                       # Add more rule options here...
#                                       ])
        
#         # Apply the selected rule function and display the resulting transactions
#         if selected_rule != "---":
#             if selected_rule == "Rule: Liquidations from the same number within a specified time window (minutes)":
#                 time_window = st.slider("Select the time window for liquidations (in minutes)", min_value=1, max_value=60, value=10)
#                 flagged_transactions = rule_liquidations_same_number(data, time_window, 'Customer', 'Biller', 'Item', 'Date')
                
#                 # Display flagged transactions with coloring
#                 st.subheader("Flagged Transactions")
#                 st.dataframe(color_by_agent(flagged_transactions))
            
#             # Repeat for other rules...

#         else:
#             st.info("Please select a rule to inspect transactions.")

# if __name__ == "__main__":
#     RulesEngine()