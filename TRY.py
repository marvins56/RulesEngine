import streamlit as st
import pandas as pd
import base64


def rule_1_time_window(data, time_window=5):
    deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
    deposits_data['date_time'] = pd.to_datetime(deposits_data['date_time'])
    deposits_data = deposits_data.sort_values(['agent_code', 'ACC/NO', 'date_time'])
    deposits_data['time_diff'] = deposits_data.groupby(['agent_code', 'ACC/NO'])['date_time'].diff().dt.total_seconds() / 60
    flagged_transactions = deposits_data['time_diff'] <= time_window
    flagged_data = deposits_data[flagged_transactions]

    # Getting the index of flagged transactions
    flagged_index = flagged_data.index

    # Removing the flagged transactions from the original data
    remaining_data = data.loc[~data.index.isin(flagged_index)]

    return flagged_data, remaining_data

def rule_2_daily_limit(data, daily_limit=2):
    deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
    deposits_data['date_time'] = pd.to_datetime(deposits_data['date_time']) # Ensure the 'date_time' column is datetime
    deposits_data['date'] = deposits_data['date_time'].dt.date # Extract the date part
    daily_transactions = deposits_data.groupby(['agent_code', 'ACC/NO', 'date']).size().reset_index(name='count')
    flagged_data = daily_transactions[daily_transactions['count'] > daily_limit]
    flagged_data['date'] = pd.to_datetime(flagged_data['date']) # Convert 'date' to datetime
    flagged_data['consistency'] = flagged_data.groupby(['agent_code', 'ACC/NO'])['date'].diff().dt.days == 1
    consistent_flagged_data = flagged_data[flagged_data['consistency']]
    return consistent_flagged_data



def generate_download_link(data, filename):
    csv = data.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}">Click here to download</a>'


def TRY():
    st.title("Transaction Rule Engine")

    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
    if uploaded_file:
        st.spinner("Reading File.....")
        
        if uploaded_file.name.endswith('.csv'):
            data = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            data = pd.read_excel(uploaded_file)
        

    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
                data = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.xlsx'):
            data = pd.read_excel(uploaded_file)
            

        rule_selection = st.selectbox("Select a rule to apply:", ["Rule 1: Time Window Rule", "Rule 2: Daily Limit Rule"])

        if rule_selection == "Rule 1: Time Window Rule":
            time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)
            flagged_data, remaining_data = rule_1_time_window(data, time_window)
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
                top_acquirer = flagged_data['Acquirer'].value_counts().idxmax()

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
            st.write(f"Acquirer: {top_acquirer}")

            # Display flagged transactions statistics for each agent
            with st.spinner("Generating Agent Statistics..."):
                top_10_agents = top_agents.index
                flagged_agents = flagged_data[flagged_data['agent_code'].isin(top_10_agents)].groupby('agent_code').agg(
                    total_flagged=('agent_code', 'count')
                )
            st.subheader("Flagged Transactions by Agent")
            st.write(flagged_agents)


            st.subheader("Flagged Transactions")
            st.write(flagged_data)

            st.subheader("Remaining Transactions")
            st.write(remaining_data)

            flagged_link = generate_download_link(flagged_data, "flagged_data.csv")
            remaining_link = generate_download_link(remaining_data, "remaining_data.csv")

            st.markdown(flagged_link, unsafe_allow_html=True)
            st.markdown(remaining_link, unsafe_allow_html=True)

        elif rule_selection == "Rule 2: Daily Limit Rule":
            daily_limit = st.slider("Daily Limit (transactions)", min_value=2, max_value=10, value=2)
            flagged_data = rule_2_daily_limit(data, daily_limit)

            st.subheader("Flagged Transactions")
            st.write(flagged_data)

            flagged_link = generate_download_link(flagged_data, "flagged_data.csv")
            st.markdown(flagged_link, unsafe_allow_html=True)


if __name__ == "__main__":
    TRY()
