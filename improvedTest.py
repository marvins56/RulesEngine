
import streamlit as st
import pandas as pd
import dask.dataframe as dd
import base64
from datetime import timedelta

def filter_transactions(data):
    try:
        # Update the keywords for filtering
        float_purchase_keywords = ['AIRTELFLOATPURCHASE_W', 'MTNFLOATPURCHASE_W', 'CDP']
        
        filtered_data = data[data['txn_type'].isin(float_purchase_keywords)]
        filtered_data = filtered_data.compute()

        # Keep only the necessary columns
        columns_to_keep = ['date_time','AGENTNAMES','ACC/NO','agent_code','txn_type','Response_code',' Amount ']
        filtered_data = filtered_data[columns_to_keep]

        successful_filtered_transactions = filtered_data[filtered_data['Response_code'] == 0]
        
        return successful_filtered_transactions
    except Exception as e:
        st.error("An error occurred while filtering repeated transactions.")
        st.write(e)
        return None


def filter_cash_deposits_and_float_purchases(data):
    try:
        # Update the keywords for filtering
        cash_deposit_keywords = ['CDP']
        float_purchase_keywords = ['AIRTELFLOATPURCHASE_W', 'MTNFLOATPURCHASE_W']
        
        cash_deposits = data[data['txn_type'].isin(cash_deposit_keywords)]
        float_purchases = data[data['txn_type'].isin(float_purchase_keywords)]
        
        return cash_deposits, float_purchases
    except Exception as e:
        st.error("An error occurred while filtering cash deposits and float purchases.")
        st.write(e)
        return None, None

def flag_agents_with_multiple_cash_deposits(data):
    try:
        # Filter data to include only cash deposits
        cash_deposits = data[data['txn_type'] == 'CDP']

        # Group data by agent and account, and count the number of cash deposits
        cash_deposits_count = cash_deposits.groupby(['agent_code', 'ACC/NO']).size().reset_index()
        cash_deposits_count = cash_deposits_count.rename(columns={0: 'num_cash_deposits'})

        # Filter agents with more than 2 cash deposits to the same account
        flagged_agents = cash_deposits_count[cash_deposits_count['num_cash_deposits'] > 2]

        # Merge the flagged_agents with the cash deposits to get the flagged transactions using an inner join
        flagged_transactions = dd.merge(cash_deposits, flagged_agents[['agent_code', 'ACC/NO']], on=['agent_code', 'ACC/NO'], how='inner')

        # Add the flagged_reason column
        flagged_transactions['flagged_reason'] = 'Multiple Cash Deposits to Same Account'
        
        Flaged = flagged_transactions[['date_time','AGENTNAMES','ACC/NO','agent_code','txn_type','Response_code',' Amount ']]

        return Flaged

    except Exception as e:
        st.error("An error occurred while flagging agents with multiple cash deposits to the same account.")
        st.write(e)
        return None

def flag_repeated_cash_deposits_transactions_within_time_range(data, time_threshold_minutes):
    try:
        # Convert date_time column to datetime format
        data['date_time'] = dd.to_datetime(data['date_time']) # Make sure you really need this line. If 'data' is already a pandas DataFrame, use pd.to_datetime instead.

        # Calculate time difference between consecutive transactions
        data = data.sort_values(['agent_code', 'ACC/NO', 'date_time'])
        data['time_diff'] = data.groupby(['agent_code', 'ACC/NO'])['date_time'].transform(lambda x: x.diff()).dt.total_seconds() / 60

        # Select transactions within the given time threshold
        flagged_transactions = data[data['time_diff'] <= time_threshold_minutes]
        
        # Add the flagged_reason column
        flagged_transactions['flagged_reason'] = 'Multiple Transactions by Same Agent to Same Account in Time Range'
        
        # Drop the time_diff column
        flagged_transactions = flagged_transactions.drop(columns=['time_diff'])

        return flagged_transactions

    except Exception as e:
        st.error("An error occurred while flagging repeated transactions within a time range.")
        st.write(e)
        return None

# def flag_repeated_cash_deposits_transactions_within_time_range(data, time_threshold_minutes):
#     try:
#         # Convert date_time column to datetime format
#         data['date_time'] = dd.to_datetime(data['date_time'])

#         # Calculate time difference between consecutive transactions
#         data = data.sort_values(['agent_code', 'ACC/NO', 'date_time'])
#         # data['time_diff'] = data.groupby(['agent_code', 'ACC/NO'])['date_time'].diff().dt.total_seconds() / 60
#         data['time_diff'] = data.groupby(['agent_code', 'ACC/NO'])['date_time'].transform(lambda x: x.diff()).dt.total_seconds() / 60

#         # Select transactions within the given time threshold
#         flagged_transactions = data[data['time_diff'] <= time_threshold_minutes]
        
#         # Add the flagged_reason column
#         flagged_transactions['flagged_reason'] = 'Multiple Transactions by Same Agent to Same Account in Time Range'
        
#         # Drop the time_diff column
#         flagged_transactions = flagged_transactions.drop(columns=['time_diff'])
#         flagged = flagged_transactions.compute()

#         return flagged

#     except Exception as e:
#         st.error("An error occurred while flagging repeated transactions within a time range.")
#         st.write(e)
#         return None


def flag_float_purchases_same_number(data, time_window_minutes):
    try:
        # Group data by agent and number, and count the number of float purchases
        # float_purchases_count = data.groupby(['agent_code', 'ACC/NO']).size().reset_index()
        float_purchases_count = data.groupby(['agent_code', 'ACC/NO']).size().reset_index(name='num_float_purchases')

        # Filter agents with more than 1 float purchase to the same number
        flagged_agents = float_purchases_count[float_purchases_count['num_float_purchases'] > 1]

        # Merge the flagged_agents with the original data to get the flagged transactions
        flagged_transactions = data.merge(flagged_agents, on=['agent_code', 'ACC/NO'])

        # Add the flagged_reason column
        flagged_transactions['flagged_reason'] = 'Multiple Float Purchases to Same Number'

        # Group flagged transactions by agents and return
        grouped_flagged_transactions = flagged_transactions.groupby(['agent_code', 'ACC/NO', 'flagged_reason'])
        grouped_flagged_transactions = grouped_flagged_transactions.size().reset_index(name='count')


        return flagged_transactions,grouped_flagged_transactions

    except Exception as e:
        st.error("An error occurred while flagging agents with multiple float purchases to the same number.")
        st.write(e)
        return None

def flag_agents_with_multiple_float_purchases(float_purchases):
    try:
        # Group float purchase data by agent and account, and filter by transactions made more than twice
        repeated_float_purchases = float_purchases.groupby(['agent_code', 'ACC/NO']).filter(lambda x: len(x) > 2)

        # Add the flagged_reason column
        repeated_float_purchases['flagged_reason'] = 'Multiple Float Purchases to Same Account'

        return repeated_float_purchases

    except Exception as e:
        st.error("An error occurred while flagging agents with multiple float purchases to the same account.")
        st.write(e)
        return None


def flag_customers_with_multiple_cash_deposits(data, time_threshold_minutes):
    try:
        # Filter data to include only cash deposits
        cash_deposits = data[data['txn_type'] == 'CDP']

        # Group data by customer account and agent, and count the number of cash deposits
        cash_deposits_count = cash_deposits.groupby(['ACC/NO', 'agent_code']).size().reset_index(name='num_cash_deposits')

        # Filter customers with more than 2 cash deposits from different agents within the time threshold
        flagged_customers = cash_deposits_count[cash_deposits_count['num_cash_deposits'] > 2]

        # Merge the flagged_customers with the original data to get the flagged transactions
        flagged_transactions = cash_deposits.merge(flagged_customers, on=['ACC/NO', 'agent_code'])

        # Add the flagged_reason column
        flagged_transactions['flagged_reason'] = 'Multiple Agents Providing Cash Deposits to Same Customer within Time Period'

        return flagged_transactions

    except Exception as e:
        st.error("An error occurred while flagging customers with multiple agents providing cash deposits within a time period.")
        st.write(e)
        return None

def flag_customers_with_multiple_float_deposits(data, time_threshold_minutes):
    try:
        # Filter data to include only float purchases
        float_purchases = data[data['txn_type'].isin(['AIRTELFLOATPURCHASE_W', 'MTNFLOATPURCHASE_W'])]

        # Group data by customer account and agent, and count the number of float purchases
        float_purchases_count = float_purchases.groupby(['ACC/NO', 'agent_code']).size().reset_index(name='num_Float_deposits')

        # Filter customers with more than 2 float purchases from different agents within the time threshold
        flagged_customers = float_purchases_count[float_purchases_count['num_float_purchases'] > 2]

        # Merge the flagged_customers with the original data to get the flagged transactions
        flagged_transactions = float_purchases.merge(flagged_customers, on=['ACC/NO', 'agent_code'])

        # Add the flagged_reason column
        flagged_transactions['flagged_reason'] = 'Multiple Agents Providing Float Purchases to Same Customer within Time Period'

        return flagged_transactions

    except Exception as e:
        st.error("An error occurred while flagging customers with multiple agents providing float purchases within a time period.")
        st.write(e)
        return None

def generate_download_link(data, filename):
    b64 = base64.b64encode(data.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}">Click here to download</a>'

# Define your functions here (filter_transactions, filter_cash_deposits_and_float_purchases, etc.)

def MainNew():
    st.title("Transaction Filter Tool")
    st.write("Upload a CSV or Excel file to filter transactions.")

    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])

    if uploaded_file:
        try:
            with st.spinner("Reading file..."):
                if uploaded_file.name.endswith('.csv'):
                    data = dd.read_csv(uploaded_file)
                elif uploaded_file.name.endswith('.xlsx'):
                    data = pd.read_excel(uploaded_file)
                    data = dd.from_pandas(data, npartitions=8)
                st.write("File uploaded successfully!")

            # Filter successful transactions
            with st.spinner("Filtering Transactions: Filters successful transactions"):
                successful_filtered_transactions = filter_transactions(data)
                
                if successful_filtered_transactions is not None:
                    st.subheader("Preview: Successful Transactions")
                    st.write(successful_filtered_transactions.head(5))  # Display a sample of 5 transactions


            # Filter cash deposits and float purchases
            with st.spinner("Filtering Cash Deposits and Float Purchases"):
                cash_deposits, float_purchases = filter_cash_deposits_and_float_purchases(successful_filtered_transactions)

                if cash_deposits is not None:
                    st.subheader("Preview: Cash Deposits")
                    st.write(cash_deposits.head(5))  # Display a sample of 5 cash deposits

                if float_purchases is not None:
                    st.subheader("Preview: Float Purchases")
                    st.write(float_purchases.head(5))  # Display a sample of 5 float purchases


            # Flag agents with multiple cash deposits
            # Flag agents with multiple cash deposits
            with st.spinner("Flagging Agents with Multiple Cash Deposits"):
                flagged_agents_cash_deposits = flag_agents_with_multiple_cash_deposits(cash_deposits)
                            
                if flagged_agents_cash_deposits is not None:
                    st.subheader("Preview: Flagged Agents with Multiple Cash Deposits")
                    st.write(flagged_agents_cash_deposits.head(5))  # Display a sample of 5 flagged transactions

                    # Check if the DataFrame is a Dask DataFrame and compute it if necessary
                    if isinstance(flagged_agents_cash_deposits, dd.DataFrame):
                        flagged_agents_cash_deposits = flagged_agents_cash_deposits.compute()

                    flagged_agents_cash_deposits_csv = flagged_agents_cash_deposits.to_csv(index=False)

                    st.subheader("Download Flagged Agents with Multiple Cash Deposits")
                    download_link_agents_cash_deposits = generate_download_link(flagged_agents_cash_deposits_csv, "flagged_agents_cash_deposits.csv")
                    st.markdown(download_link_agents_cash_deposits, unsafe_allow_html=True)


                        
            # Flag repeated cash deposits transactions within time range
            with st.spinner("Flagging Repeated Cash Deposits Transactions Within Time Range"):
                time_threshold_minutes = st.slider("Time Threshold (minutes)", min_value=1, max_value=60, value=15)
                flagged_repeated_cash_deposits = flag_repeated_cash_deposits_transactions_within_time_range(cash_deposits, time_threshold_minutes)
               
                if flagged_repeated_cash_deposits is not None:
                    st.subheader("Preview: Flagged Repeated Cash Deposits within Time Range")
                    st.write(flagged_repeated_cash_deposits.head(5))  # Display a sample of 5 flagged transactions
                    flagged_repeated_cash_deposits_csv = flagged_repeated_cash_deposits.to_csv(index=False)
                    st.subheader("Download Flagged Repeated Cash Deposits within Time Range")
                    download_link_repeated_cash_deposits = generate_download_link(flagged_repeated_cash_deposits_csv, "flagged_repeated_cash_deposits.csv")
                    st.markdown(download_link_repeated_cash_deposits, unsafe_allow_html=True)


            # Flag float purchases to same number
            with st.spinner("Flagging Float Purchases to Same Number"):
                time_window_minutes = st.slider("Time Window (minutes)", min_value=1, max_value=60, value=15)
                flagged_transactions,flagged_float_purchases_same_number = flag_float_purchases_same_number(float_purchases, time_window_minutes)
                
                if flagged_float_purchases_same_number is not None:
                    st.subheader("Preview: Flagged Float Purchases to Same Number")
                    st.write(flagged_float_purchases_same_number.head(5))  # Display a sample of 5 flagged transactions
                    
                    flagged_float_purchases_same_number_csv = flagged_transactions.to_csv(index=False)
                    st.subheader("Download Flagged Float Purchases to Same Number")
                    download_link_float_purchases_same_number = generate_download_link(flagged_float_purchases_same_number_csv, "flagged_float_purchases_same_number.csv")
                    st.markdown(download_link_float_purchases_same_number, unsafe_allow_html=True)


            # Flag agents with multiple float purchases
            with st.spinner("Flagging Agents with Multiple Float Purchases"):
                flagged_agents_float_purchases = flag_agents_with_multiple_float_purchases(float_purchases)

                if flagged_agents_float_purchases is not None:
                    st.subheader("Preview: Flagged Agents with Multiple Float Purchases")
                    st.write(flagged_agents_float_purchases.head(5))  # Display a sample of 5 flagged transactions
                    
                    flagged_agents_float_purchases_csv = flagged_agents_float_purchases.to_csv(index=False)
                    st.subheader("Download Flagged Agents with Multiple Float Purchases")
                    download_link_agents_float_purchases = generate_download_link(flagged_agents_float_purchases_csv, "flagged_agents_float_purchases.csv")
                    st.markdown(download_link_agents_float_purchases, unsafe_allow_html=True)

            # Flag customers with multiple cash deposits
            with st.spinner("Flagging Customers with Multiple Cash Deposits"):
                flagged_customers_multiple_cash_deposits = flag_customers_with_multiple_cash_deposits(cash_deposits, time_threshold_minutes)
               
                if flagged_customers_multiple_cash_deposits is not None:
                    st.subheader("Preview: Flagged Customers with Multiple Cash Deposits")
                    st.write(flagged_customers_multiple_cash_deposits.head(5))  # Display a sample of 5 flagged transactions
                    
                    flagged_customers_cash_deposits_csv = flagged_customers_multiple_cash_deposits.to_csv(index=False)
                    st.subheader("Download Flagged Customers with Multiple Cash Deposits")
                    download_link_customers_cash_deposits = generate_download_link(flagged_customers_cash_deposits_csv, "flagged_customers_cash_deposits.csv")
                    st.markdown(download_link_customers_cash_deposits, unsafe_allow_html=True)


            # Flag customers with multiple float deposits
            with st.spinner("Flagging Customers with Multiple Float Deposits"):
                flagged_customers_multiple_float_deposits = flag_customers_with_multiple_float_deposits(float_purchases, time_threshold_minutes)
                if flagged_customers_multiple_float_deposits is not None:
                    st.subheader("Preview: Flagged Customers with Multiple Float Deposits")
                    st.write(flagged_customers_multiple_float_deposits.head(5))  # Display a sample of 5 flagged transactions
                    
                    flagged_customers_float_deposits_csv = flagged_customers_multiple_float_deposits.to_csv(index=False)
                    st.subheader("Download Flagged Customers with Multiple Float Deposits")
                    download_link_customers_float_deposits = generate_download_link(flagged_customers_float_deposits_csv, "flagged_customers_float_deposits.csv")
                    st.markdown(download_link_customers_float_deposits, unsafe_allow_html=True)

            # # Display flagged transactions and provide download links
            # # You can use st.write() to display the flagged transactions in each case
            

        except Exception as e:
            st.error("An error occurred while processing the file.")
            st.write(e)

if __name__ == "__main__":
    MainNew()
