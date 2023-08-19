
import streamlit as st
import pandas as pd
import dask.dataframe as dd
import base64
from datetime import datetime, timedelta



def filter_repeated_transactions(data):
    try:
        filtered_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
        filtered_data = filtered_data.compute()

        # Keep only the necessary columns
        columns_to_keep = ['agent_code', 'ACC/NO', 'txn_type', 'Response_code', 'date_time','AGENTNAMES',' Amount ']
        filtered_data = filtered_data[columns_to_keep]

        repeated_transactions = filtered_data.groupby(['agent_code', 'ACC/NO']).filter(lambda x: len(x) > 2)
        successful_repeated_transactions = repeated_transactions[repeated_transactions['Response_code'] == 0]
        return successful_repeated_transactions
    except Exception as e:
        st.error("An error occurred while filtering repeated transactions.")
        st.write(e)
        return None

def generate_download_link(data, filename):
    b64 = base64.b64encode(data.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}">Click here to download</a>'

def filter_float_purchases(transactions, times_threshold):
    try:
        float_purchases = transactions[transactions['txn_type'] == 'FLOAT_DEPOSIT']
        repeated_float_purchases = float_purchases.groupby(['agent_code', 'ACC/NO']).filter(lambda x: len(x) > times_threshold)
        return repeated_float_purchases
    except Exception as e:
        st.error("An error occurred while filtering float purchases.")
        st.write(e)
        return None

def filter_cdp_transactions(transactions, times_threshold):
    try:
        cdp_transactions = transactions[transactions['txn_type'] == 'CDP']
        repeated_cdp_transactions = cdp_transactions.groupby(['agent_code', 'ACC/NO']).filter(lambda x: len(x) > times_threshold)
        return repeated_cdp_transactions
    except Exception as e:
        st.error("An error occurred while filtering CDP transactions.")
        st.write(e)
        return None


def flag_transactions(transactions, transaction_type, reason_code_counter):
    # Function to set the flagged reason
    def set_flagged_reason(row):
        if transaction_type == 'FLOAT_DEPOSIT':
            return 'Multiple Float Purchases to Same Account'
        elif transaction_type == 'CDP':
            return 'Multiple Cash Deposits to Same Account'
        else:
            return 'Unknown'

    # Add the flagged_reason column
    transactions['flagged_reason'] = transactions.apply(set_flagged_reason, axis=1)

    # Group by agent and account, then filter by the time difference within 15 minutes
    transactions['date_time'] = pd.to_datetime(transactions['date_time'])
    grouped = transactions.groupby(['agent_code', 'ACC/NO'])
    filtered_groups = []
    for name, group in grouped:
        group = group.sort_values('date_time')
        group['time_diff'] = group['date_time'].diff()
        group_within_15mins = group[group['time_diff'].lt(timedelta(minutes=15)) | group['time_diff'].isna()]

        # If the group has transactions within 15 minutes
        if not group_within_15mins.empty:
            reason_code = f"RC{str(reason_code_counter).zfill(2)}"
            group_within_15mins.loc[:, 'reason_code'] = reason_code  # Use .loc to assign the value
            
            filtered_groups.append(group_within_15mins)
            reason_code_counter += 1

    flagged_data = pd.DataFrame()  # Initialize as empty DataFrame
    if filtered_groups:  # Check if filtered_groups is not empty
        flagged_data = pd.concat(filtered_groups, ignore_index=True)

    # Drop the time_diff column
    flagged_data.drop(columns=['time_diff'], inplace=True)

    return flagged_data, reason_code_counter

def combine_and_flag_transactions(float_purchases, cdp_transactions):
    reason_code_counter = 1

    # Flag float purchases
    flagged_float_purchases, reason_code_counter = flag_transactions(float_purchases, 'FLOAT_DEPOSIT', reason_code_counter)

    # Flag CDP transactions
    flagged_cdp_transactions, reason_code_counter = flag_transactions(cdp_transactions, 'CDP', reason_code_counter)

    # Combine flagged transactions
    combined_flagged_data = pd.concat([flagged_float_purchases, flagged_cdp_transactions], ignore_index=True)

    return combined_flagged_data



def main_ui():
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

            with st.spinner("Filtering Transactions: Filters transactions for FLOAT_DEPOSIT and CDP."):
                repeated_transactions = filter_repeated_transactions(data)

            times_threshold = st.slider("Time Period (times)", min_value=3, max_value=10, value=3)

            with st.spinner("Filtering FLOAT_DEPOSIT Transactions: Filters FLOAT_DEPOSIT transactions based on a threshold."):
                float_purchases = filter_float_purchases(repeated_transactions, times_threshold)

            with st.spinner("Filtering CDP transactions..."):
                cdp_transactions = filter_cdp_transactions(repeated_transactions, times_threshold)


            # Combine and flag the FLOAT_DEPOSIT and CDP transactions
            with st.spinner("Combining and Flagging Transactions: Combines FLOAT_DEPOSIT and CDP transactions and flags them based on specific criteria"):
                flagged_data = combine_and_flag_transactions(float_purchases, cdp_transactions)

            
                
            if repeated_transactions is not None:
                st.subheader("Preview: Repeated Transactions (FLOAT_DEPOSIT and CDP)")
                st.write(repeated_transactions.head(10))  # Preview first 10 rows

            if float_purchases is not None:
                st.subheader(f"Preview: FLOAT_DEPOSIT Transactions More Than {times_threshold} Times")
                st.write(float_purchases.head(10))  # Preview first 10 rows

            if cdp_transactions is not None:
                st.subheader(f"Preview: CDP Transactions More Than {times_threshold} Times")
                st.write(cdp_transactions.head(10))  # Preview first 10 rows
          
            # Preview and download the combined and flagged transactions
            if flagged_data is not None:
                st.subheader("Preview: Combined Flagged Transactions (Multiple Float Purchases & Cash Deposits to Same Account)")
                st.write(flagged_data.head(10))  # Preview first 10 rows

            repeated_csv = repeated_transactions.to_csv(index=False) if repeated_transactions is not None else None
            float_csv = float_purchases.to_csv(index=False) if float_purchases is not None else None
            cdp_csv = cdp_transactions.to_csv(index=False) if cdp_transactions is not None else None
            flagged_csv = flagged_data.to_csv(index=False) if flagged_data is not None else None

            st.subheader("Download Results")

            if flagged_csv:
    
                st.write("Download all flagged transactions (FLOAT_DEPOSIT and CDP)")
                download_link_flagged = generate_download_link(flagged_csv, "flagged_transactions.csv")
                st.markdown(download_link_flagged, unsafe_allow_html=True)
            # # Convert flagged data to CSV for download
                   
            # if repeated_csv:
            #     st.write("Download all transactions for FLOAT_DEPOSIT and CASH DEPOSITS")
            #     download_link_repeated = generate_download_link(repeated_csv, "repeated_transactions.csv")
            #     st.markdown(download_link_repeated, unsafe_allow_html=True)

            if float_csv:
                st.write("Download all flagged transactions for FLOAT_DEPOSIT")
                download_link_float = generate_download_link(float_csv, "float_purchases.csv")
                st.markdown(download_link_float, unsafe_allow_html=True)

            if cdp_csv:
                st.write("Download all flagged transactions for CDP")
                download_link_cdp = generate_download_link(cdp_csv, "cdp_transactions.csv")
                st.markdown(download_link_cdp, unsafe_allow_html=True)

        except Exception as e:
            st.error("An error occurred while processing the file.")
            st.write(e)

if __name__ == "__main__":
    main_ui()
