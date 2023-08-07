import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import base64
import plotly.graph_objs as go
import numpy as np
import random
import os

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


# Function to save DataFrame to a CSV file in the specified folder with date and time in the file name
def save_to_folder_with_datetime(data, folder_name, reason):
    os.makedirs(folder_name, exist_ok=True)
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{current_datetime}_{reason}.csv"
    file_path = os.path.join(folder_name, file_name)
    data.to_csv(file_path, index=False)

# Function to save results to specific subfolders within the main folder
def save_results_to_folders(data, main_folder, rule_name, folder_name, file_name):
    rule_folder = os.path.join(main_folder, rule_name)
    subfolder = os.path.join(rule_folder, folder_name)
    file_path = os.path.join(subfolder, file_name)
    os.makedirs(subfolder, exist_ok=True)
    data.to_csv(file_path, index=False)

def rule_liquidations_same_number(data, time_window_minutes, number_column, biller_column, item_column, timestamp_column):
    # Filter data for Mobile Money Liquidations with Airtel Float or MTN Float
    mask_liquidations = (
        data[biller_column].str.contains('Mobile Money Liquidation') &
        (data[item_column].str.contains('Airtel Float Liquidation') | data[item_column].str.contains('MTN Float Liquidation'))
    )

    # Convert timestamp column to datetime
    data[timestamp_column] = pd.to_datetime(data[timestamp_column])

    # Create a DataFrame containing only liquidation transactions
    liquidations_data = data[mask_liquidations].sort_values(by=timestamp_column)

    # Find the indices of transactions that have previous transactions within the time window
    flagged_indices = []
    for index, transaction in liquidations_data.iterrows():
        mask_time_window = (
            (liquidations_data[number_column] == transaction[number_column]) &
            (liquidations_data[timestamp_column] >= transaction[timestamp_column] - pd.Timedelta(minutes=time_window_minutes)) &
            (liquidations_data[timestamp_column] < transaction[timestamp_column])
        )
        if liquidations_data[mask_time_window].shape[0] > 0:
            flagged_indices.append(index)

    # Get flagged and good data based on indices
    flagged_data = data.loc[flagged_indices]
    good_data = data.drop(index=flagged_indices)

    # Save flagged and good data to separate folders within the Rule1 folder with date and time in the file name
    save_results_to_folders(flagged_data, "Results", "Rule1", "Flagged", "flagged")
    save_results_to_folders(good_data, "Results", "Rule1", "Good", "good")

    return flagged_data, good_data

#     return flagged_data_filtered, good_data
def rule_deposit_or_float_same_account_time_window(data, time_window, agent_column, account_column, date_column):
    # Ensure the date column is in datetime format
    data[date_column] = pd.to_datetime(data[date_column])

    # Set the date column as the index and sort it
    data = data.set_index(date_column).sort_index()

    # Keywords for filtering
    bank_deposit_keywords = ['deposit', 'deposits', 'bank']
    mtn_float_keywords = ['MTN Float', 'MTN Float purchase']
    airtel_float_keywords = ['Airtel Float', 'Airtel Float purchase']

    # Create masks for filtering
    bank_deposit_mask = data[account_column].str.contains('|'.join(bank_deposit_keywords), case=False, na=False)
    mtn_float_mask = data[account_column].str.contains('|'.join(mtn_float_keywords), case=False, na=False)
    airtel_float_mask = data[account_column].str.contains('|'.join(airtel_float_keywords), case=False, na=False)

    # Apply masks to filter data
    filtered_data = pd.concat([data[bank_deposit_mask], data[mtn_float_mask], data[airtel_float_mask]])

    # Initialize an empty DataFrame to store flagged transactions
    flagged_transactions = pd.DataFrame()

    # Iterate through each transaction in the sorted data
    for date, transaction in filtered_data.iterrows():
        # Find transactions made by the same agent to the same account within the time window
        mask = (filtered_data[agent_column] == transaction[agent_column]) & \
               (filtered_data[account_column] == transaction[account_column]) & \
               (filtered_data.index >= date) & \
               (filtered_data.index <= date + pd.Timedelta(minutes=time_window))

        # Check if there are any other transactions within the time window
        if len(filtered_data[mask]) > 1:
            flagged_transactions = flagged_transactions.append(filtered_data[mask])

    # Remove duplicates from flagged transactions
    # flagged_transactions = flagged_transactions.drop_duplicates()

    # Get the good (unflagged) transactions by excluding the flagged ones
    good_transactions = data.loc[~data.index.isin(flagged_transactions.index)]

    # Reset the index for the final result
    flagged_transactions.reset_index(inplace=True)
    good_transactions.reset_index(inplace=True)

    return flagged_transactions, good_transactions


# Placeholder column names (replace with actual column names from your data)
number_column = "Customer"  # Example, update as needed
biller_column = "Biller"    # Example, update as needed
item_column = "Item"           # Example, update as needed
timestamp_column = "Date"             # Example, update as needed
terminal_column = "Terminal"
name_column = "Name"


def SequentialRulesEngine():
    st.title("Sequential Rules Engine")
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])

    if uploaded_file is not None:
        with st.spinner('Loading data...'):
            data = load_data(uploaded_file)  # Make sure to define or import this function
            data = clean_data(data)          # Make sure to define or import this function
        st.write("Data Overview:")
        st.write(f"Total Transactions: {len(data)}")
        st.write(data.head())
        st.markdown("---")  # Horizontal line
        st.write("Results:")

        # Code for Rule 1 (keep as is or replace with your implementation)
        time_window_minutes_rule1 = st.slider("Time Window for Rule 1 (minutes):", 1, 15, 5)
        with st.spinner('Applying Rule 1: Liquidations from the same number within a specified time window (minutes)'):
            flagged_data_rule1, good_data_rule1 = rule_liquidations_same_number(
                data, time_window_minutes_rule1, number_column, biller_column, item_column, timestamp_column)
            save_results_to_folders(flagged_data_rule1, "Results", "Rule1", "Flagged", "flagged.csv")
            save_results_to_folders(good_data_rule1, "Results", "Rule1", "Good", "good.csv")
        st.write(f"Flagged Transactions (Rule 1): {len(flagged_data_rule1)}")
        st.write(f"Good Transactions (Rule 1): {len(good_data_rule1)}")

      # Display flagged transactions in dropdowns for each agent (Rule 1)
        st.write("Flagged Transactions (Rule 1):")
        unique_agents_rule1 = flagged_data_rule1[number_column].unique()
        for agent_number in unique_agents_rule1:
            agent_transactions = flagged_data_rule1[flagged_data_rule1[number_column] == agent_number]
            terminal_number = agent_transactions[terminal_column].iloc[0]
            agent_name = agent_transactions[name_column].iloc[0]
            total_transactions = len(agent_transactions)
            if st.checkbox(f"Agent: {agent_name} | Terminal: {terminal_number} | Customer Acct: {agent_number} | Transactions: {total_transactions} (Rule 1)"):
                st.write(agent_transactions)
                st.markdown("---")


        # Loading good data from Rule 1
        with st.spinner('Loading good data from Rule 1...'):
            good_data_path_rule1 = os.path.join("Results", "Rule1", "Good", "good.csv")
            good_data_rule1 = pd.read_csv(good_data_path_rule1)

        # Code for Rule 2 using the refactored function
        time_window_minutes_rule2 = st.slider("Time Window for Rule 2 (minutes):", 1, 15, 5)
        with st.spinner('Applying Rule 2:Deposits or Float purchases to the same account within a specified time window (minutes)'):
            flagged_data_rule2, good_data_rule2 = rule_deposit_or_float_same_account_time_window(
                good_data_rule1, time_window_minutes_rule2, number_column, biller_column, timestamp_column)
            save_results_to_folders(flagged_data_rule2, "Results", "Rule2", "Flagged", "flagged.csv")
            save_results_to_folders(good_data_rule2, "Results", "Rule2", "Good", "good.csv")
            st.write(f"Flagged Transactions (Rule 2): {len(flagged_data_rule2)}")
            st.write(f"Good Transactions (Rule 2): {len(good_data_rule2)}")
                        # Display flagged transactions in dropdowns for each agent (Rule 2)
            st.write("Flagged Transactions (Rule 2):")
            unique_agents_rule2 = flagged_data_rule2[number_column].unique()
            for agent_number in unique_agents_rule2:
                agent_transactions = flagged_data_rule2[flagged_data_rule2[number_column] == agent_number]
                terminal_number = agent_transactions[terminal_column].iloc[0]
                agent_name = agent_transactions[name_column].iloc[0]
                total_transactions = len(agent_transactions)
                if st.checkbox(f"Agent: {agent_name} | Terminal: {terminal_number} | Customer Acct: {agent_number} | Transactions: {total_transactions} (Rule 2)"):
                    st.write(agent_transactions)
                    st.markdown("---")
            # Add buttons to download the final clean data and flagged data for each rule
        st.sidebar.subheader("Download Data")
        if st.sidebar.button("Download Clean Data"):
            st.sidebar.download_button("Click to Download Clean Data", data.to_csv(), file_name="clean_data.csv", mime="text/csv")

        if st.sidebar.button("Download Flagged Data (Rule 1)"):
            flagged_data_path_rule1 = os.path.join("Results", "Rule1", "Flagged", "flagged.csv")
            st.sidebar.download_button("Click to Download Flagged Data (Rule 1)", flagged_data_rule1.to_csv(), file_name="flagged_data_rule1.csv", mime="text/csv")

        if st.sidebar.button("Download Flagged Data (Rule 2)"):
            flagged_data_path_rule2 = os.path.join("Results", "Rule2", "Flagged", "flagged.csv")
            st.sidebar.download_button("Click to Download Flagged Data (Rule 2)", flagged_data_rule2.to_csv(), file_name="flagged_data_rule2.csv", mime="text/csv")


        # Continue with subsequent rules, using spinners as needed

    else:
        st.warning("Please upload a CSV or Excel file.")


# Uncomment to run the app (only if running locally)
if __name__ == "__main__":
    SequentialRulesEngine()
