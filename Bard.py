import streamlit as st
import dask.dataframe as dd
import pandas as pd
import base64

def check_deposits_within_time_window(data, time_window):
    try:
        deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
        deposits_data['date_time'] = dd.to_datetime(deposits_data['date_time'])
        deposits_data = deposits_data.compute()
        deposits_data = deposits_data.sort_values(['agent_code', 'ACC/NO', 'date_time'])
        deposits_data['time_diff'] = deposits_data.groupby(['agent_code', 'ACC/NO'])['date_time'].diff().dt.total_seconds() / 60
        flagged_transactions = deposits_data['time_diff'] <= time_window
        flagged_data = deposits_data[flagged_transactions]
        
        # Filter out only successful transactions from flagged data
        flagged_data = flagged_data[flagged_data['Response_code'] == 0]

        # Convert ' Amount ' column to numeric, replacing non-numeric values with 0
        flagged_data[' Amount '] = pd.to_numeric(flagged_data[' Amount '], errors='coerce').fillna(0)
        
        # Getting the index of flagged transactions
        flagged_index = flagged_data.index
        
        # Removing the flagged transactions from the original data
        remaining_data = data.loc[~data.index.isin(flagged_index)]

        return flagged_data, remaining_data
    except Exception as e:
        st.error("An error occurred while processing data.")
        st.write(e)
        return None, None

def generate_download_link(data, filename):
    b64 = base64.b64encode(data.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}">Click here to download</a>'


def NEW():
    st.title("Transaction Rule Engine")
    st.write("1. Deposits / Float purchase to same account by same agent within 5 minutes or less")

    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
    time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)
    
    if uploaded_file:
        try:
            with st.spinner("Reading File....."):
                if uploaded_file.name.endswith('.csv'):
                    data = dd.read_csv(uploaded_file)
                elif uploaded_file.name.endswith('.xlsx'):
                    data = pd.read_excel(uploaded_file)
                    data = dd.from_pandas(data, npartitions=8)
            
            with st.spinner("Data processing is in progress..."):
                flagged_data, remaining_data = check_deposits_within_time_window(data, time_window)
                
            if flagged_data is not None and remaining_data is not None:
                st.success("Data processing completed!")
                
                # Calculate and display statistics
                with st.spinner("Calculating Statistics..."):
                    total_flagged = len(flagged_data)
                    unique_agents = flagged_data['agent_code'].nunique()
                    total_amount_flagged = flagged_data[' Amount '].sum()
                    top_agents = flagged_data.groupby('agent_code').agg(
                        total_transactions=('agent_code', 'count'),
                        total_amount=(' Amount ', 'sum')
                    ).nlargest(5, 'total_transactions')
                    top_channels = flagged_data['Channel'].value_counts().nlargest(5)
                    top_acquirers = flagged_data['Acquirer'].value_counts().nlargest(5)  # Top 3 acquirers
                # General Statistics
                st.subheader("Statistics for Flagged Transactions")
                st.write(f"Total Flagged Transactions: {total_flagged}")
                st.write(f"Unique Agents Involved: {unique_agents}")
                st.write(f"Total Amount Involved in Flagged Transactions: {total_amount_flagged:.2f}")

                # Top Agents
                st.subheader("Top Agents with Most Flagged Transactions")
                st.write(top_agents)

                # Top Channels
                st.subheader("Top Channels in Flagged Transactions")
                st.write(top_channels)

                # Acquirer
                st.subheader("Acquirer with Most Flagged Transactions")
                st.table({"Acquirer": top_acquirers.tolist()})  # Convert to list before displaying in table

                # Display flagged transactions statistics for each agent
                with st.spinner("Generating Agent Statistics..."):
                    top_10_agents = top_agents.index
                    flagged_agents = flagged_data[flagged_data['agent_code'].isin(top_10_agents)].groupby('agent_code').agg(
                        total_flagged=('agent_code', 'count')
                    )
                st.subheader("Top Flagged Transactions by Agent")
                st.write(flagged_agents)

                # Convert Dask DataFrames to CSV strings
                flagged_csv = flagged_data.to_csv(index=False)
                remaining_data_pd = remaining_data.compute()  # Convert to pandas DataFrame
                remaining_csv = remaining_data_pd.to_csv(index=False)  # Convert to CSV string

                # Generate file download links
                flagged_filename = "flagged_data.csv"
                flagged_link = generate_download_link(flagged_csv, flagged_filename)
                remaining_filename = "remaining_data.csv"
                remaining_link = generate_download_link(remaining_csv, remaining_filename)

                # Sidebar section for download links
                 # Sidebar section for download links
                st.subheader("Download Data")
                st.write("Download flagged data:")
                st.markdown(flagged_link, unsafe_allow_html=True)
                st.write("Download remaining data:")
                st.markdown(remaining_link, unsafe_allow_html=True)
                    
        except Exception as e:
            st.error("An error occurred while processing data.")
            st.write(e)
               
if __name__ == "__main__":
    NEW()