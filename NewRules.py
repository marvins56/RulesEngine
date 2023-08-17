

# # # # import streamlit as st
# # # # import dask.dataframe as dd
# # # # import pandas as pd

# # # # def check_deposits_within_time_window(data, time_window):
# # # #     deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
# # # #     deposits_data['date_time'] = dd.to_datetime(deposits_data['date_time'])
# # # #     deposits_data = deposits_data.compute()
# # # #     deposits_data = deposits_data.sort_values(['agent_code', 'date_time'])
# # # #     deposits_data['time_diff'] = deposits_data.groupby('agent_code')['date_time'].diff().dt.total_seconds() / 60
# # # #     flagged_transactions = deposits_data['time_diff'] <= time_window
# # # #     flagged_data = deposits_data[flagged_transactions]
# # # #     remaining_data = deposits_data[~flagged_transactions]
# # # #     return flagged_data, remaining_data

# # # # def main():
# # # #     st.title("Transaction Rule Engine")

# # # #     uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
# # # #     time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)

# # # #     if uploaded_file:
# # # #         if uploaded_file.name.endswith('.csv'):
# # # #             data = dd.read_csv(uploaded_file)
# # # #         elif uploaded_file.name.endswith('.xlsx'):
# # # #             data = pd.read_excel(uploaded_file)
# # # #             data = dd.from_pandas(data, npartitions=2)

# # # #         flagged_data, remaining_data = check_deposits_within_time_window(data, time_window)

# # # #         st.subheader("Flagged Transactions")
# # # #         flagged_agents = flagged_data['agent_code'].unique()
# # # #         for agent in flagged_agents:
# # # #             agent_transactions = flagged_data[flagged_data['agent_code'] == agent]
# # # #             st.write(f"Flagged Transactions for Agent {agent}")
# # # #             st.write(agent_transactions)

# # # #         st.subheader("Remaining Transactions")
# # # #         st.write(remaining_data)

# # # #         st.download_button(label="Download Flagged Data as CSV", data=flagged_data.to_csv(index=False), file_name="flagged_data.csv", mime="text/csv")
# # # #         st.download_button(label="Download Remaining Data as CSV", data=remaining_data.to_csv(index=False), file_name="remaining_data.csv", mime="text/csv")

# # # # if __name__ == "__main__":
# # # #     main()


# # # import streamlit as st
# # # import pandas as pd

# # # def check_deposits_within_time_window(data, time_window):
# # #     deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
# # #     deposits_data['date_time'] = pd.to_datetime(deposits_data['date_time'])
# # #     deposits_data = deposits_data.sort_values(['agent_code', 'date_time'])
# # #     deposits_data['time_diff'] = deposits_data.groupby('agent_code')['date_time'].diff().dt.total_seconds() / 60
# # #     flagged_transactions = deposits_data['time_diff'] <= time_window
# # #     flagged_data = deposits_data[flagged_transactions]
# # #     remaining_data = deposits_data[~flagged_transactions]
# # #     return flagged_data, remaining_data

# # # def generate_excel_report(flagged_data):
# # #     excel_file = 'report.xlsx'
# # #     with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
# # #         # Summary Sheet
# # #         summary = {
# # #             'Total Flagged Transactions': [len(flagged_data)],
# # #             'Number of Agents Involved': [flagged_data['agent_code'].nunique()],
# # #             'Total Amount Flagged': [flagged_data[' Amount '].sum()]
# # #         }
# # #         pd.DataFrame(summary).to_excel(writer, sheet_name='Summary', index=False)

# # #         # Top Agents Sheet
# # #         top_agents = flagged_data['agent_code'].value_counts().reset_index()
# # #         top_agents.columns = ['Agent Code', 'Count']
# # #         top_agents.to_excel(writer, sheet_name='Top Agents', index=False)

# # #         # Transaction Types Sheet
# # #         txn_types = flagged_data['txn_type'].value_counts().reset_index()
# # #         txn_types.columns = ['Transaction Type', 'Count']
# # #         txn_types.to_excel(writer, sheet_name='Transaction Types', index=False)

# # #         # Time Distribution Sheet
# # #         time_distribution = flagged_data['date_time'].dt.hour.value_counts().reset_index()
# # #         time_distribution.columns = ['Hour of Day', 'Count']
# # #         time_distribution.to_excel(writer, sheet_name='Time Distribution', index=False)

# # #         # Response Codes Sheet
# # #         response_codes = flagged_data['Response_code'].value_counts().reset_index()
# # #         response_codes.columns = ['Response Code', 'Count']
# # #         response_codes.to_excel(writer, sheet_name='Response Codes', index=False)

# # #     return excel_file

# # # def main():
# # #     st.title("Transaction Rule Engine")

# # #     uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
# # #     time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)

# # #     if uploaded_file:
# # #         if uploaded_file.name.endswith('.csv'):
# # #             data = pd.read_csv(uploaded_file)
# # #         elif uploaded_file.name.endswith('.xlsx'):
# # #             data = pd.read_excel(uploaded_file, engine='openpyxl')

# # #         flagged_data, remaining_data = check_deposits_within_time_window(data, time_window)

# # #         excel_file = generate_excel_report(flagged_data)
# # #         st.download_button(label="Download Report as Excel", data=open(excel_file, 'rb').read(), file_name="report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# # #         st.subheader("Flagged Transactions")
# # #         st.dataframe(flagged_data)

# # #         st.subheader("Remaining Transactions")
# # #         st.dataframe(remaining_data)

# # # if __name__ == "__main__":
# # #     main()
# # import streamlit as st
# # import dask.dataframe as dd
# # import pandas as pd

# # def check_deposits_within_time_window(data, time_window):
# #     deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
# #     deposits_data['date_time'] = dd.to_datetime(deposits_data['date_time'])
# #     deposits_data = deposits_data.compute()
# #     deposits_data = deposits_data.sort_values(['agent_code', 'date_time'])
# #     deposits_data['time_diff'] = deposits_data.groupby('agent_code')['date_time'].diff().dt.total_seconds() / 60
# #     flagged_transactions = deposits_data['time_diff'] <= time_window
# #     flagged_data = deposits_data[flagged_transactions]
# #     remaining_data = deposits_data[~flagged_transactions]
# #     return flagged_data, remaining_data

# # def generate_excel_report(flagged_data):
# #     excel_file = 'report.xlsx'
# #     with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
# #         # Summary Sheet
# #         summary = {
# #             'Total Flagged Transactions': [len(flagged_data)],
# #             'Number of Agents Involved': [flagged_data['agent_code'].nunique()],
# #             'Total Amount Flagged': [flagged_data[' Amount '].sum()]
# #         }
# #         pd.DataFrame(summary).to_excel(writer, sheet_name='Summary', index=False)

# #         # Top Agents Sheet
# #         top_agents = flagged_data['agent_code'].value_counts().reset_index()
# #         top_agents.columns = ['Agent Code', 'Count']
# #         top_agents.to_excel(writer, sheet_name='Top Agents', index=False)

# #         # Transaction Types Sheet
# #         txn_types = flagged_data['txn_type'].value_counts().reset_index()
# #         txn_types.columns = ['Transaction Type', 'Count']
# #         txn_types.to_excel(writer, sheet_name='Transaction Types', index=False)

# #         # Time Distribution Sheet
# #         time_distribution = flagged_data['date_time'].dt.hour.value_counts().reset_index()
# #         time_distribution.columns = ['Hour of Day', 'Count']
# #         time_distribution.to_excel(writer, sheet_name='Time Distribution', index=False)

# #         # Response Codes Sheet
# #         response_codes = flagged_data['Response_code'].value_counts().reset_index()
# #         response_codes.columns = ['Response Code', 'Count']
# #         response_codes.to_excel(writer, sheet_name='Response Codes', index=False)

# #     return excel_file

# # def main():
# #     st.title("Transaction Rule Engine")

# #     uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
# #     time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)

# #     if uploaded_file:
# #         if uploaded_file.name.endswith('.csv'):
# #             data = dd.read_csv(uploaded_file)
# #         elif uploaded_file.name.endswith('.xlsx'):
# #             data = pd.read_excel(uploaded_file, engine='openpyxl')
# #             data = dd.from_pandas(data, npartitions=2)

# #         flagged_data, remaining_data = check_deposits_within_time_window(data, time_window)

# #         excel_file = generate_excel_report(flagged_data)
# #         st.download_button(label="Download Report as Excel", data=open(excel_file, 'rb').read(), file_name="report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# #         st.subheader("Flagged Transactions")
# #         st.dataframe(flagged_data)

# #         st.subheader("Remaining Transactions")
# #         st.dataframe(remaining_data)

# # if __name__ == "__main__":
# #     main()
# import streamlit as st
# import dask.dataframe as dd
# import pandas as pd

# def check_deposits_within_time_window(data, time_window):
#     deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
#     deposits_data['date_time'] = dd.to_datetime(deposits_data['date_time'])
#     deposits_data = deposits_data.compute()
#     deposits_data = deposits_data.sort_values(['agent_code', 'date_time'])
#     deposits_data['time_diff'] = deposits_data.groupby('agent_code')['date_time'].diff().dt.total_seconds() / 60
#     flagged_transactions = deposits_data['time_diff'] <= time_window
#     flagged_data = deposits_data[flagged_transactions]
#     remaining_data = deposits_data[~flagged_transactions]
#     return flagged_data, remaining_data

# def generate_excel_report(flagged_data):
#     excel_file = 'report.xlsx'
#     with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
#         # Summary Sheet
#         summary_stats = {
#             'Total Flagged Transactions': len(flagged_data),
#             'Number of Agents Involved': flagged_data['agent_code'].nunique(),
#             'Total Amount Flagged': flagged_data[' Amount '].sum(),
#         }
#         pd.DataFrame.from_dict(summary_stats, orient='index', columns=['Value']).to_excel(writer, sheet_name='Summary', index=True)

#         # Top Agents Sheet
#         top_agents = flagged_data['agent_code'].value_counts().reset_index()
#         top_agents.columns = ['Agent', 'Flagged Transactions']
#         top_agents.to_excel(writer, sheet_name='Top Agents', index=False)

#         # Transaction Types Sheet
#         transaction_types_stats = flagged_data['txn_type'].value_counts().reset_index()
#         transaction_types_stats.columns = ['Transaction Type', 'Count']
#         transaction_types_stats['Percentage'] = (transaction_types_stats['Count'] / len(flagged_data)) * 100
#         transaction_types_stats.to_excel(writer, sheet_name='Transaction Types', index=False)

#         # ... (other sheets as before)

#     return excel_file

# def main():
#     st.title("Transaction Rule Engine")

#     uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
#     time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)

#     if uploaded_file:
#         if uploaded_file.name.endswith('.csv'):
#             data = dd.read_csv(uploaded_file)
#         elif uploaded_file.name.endswith('.xlsx'):
#             data = pd.read_excel(uploaded_file, engine='openpyxl')
#             data = dd.from_pandas(data, npartitions=2)

#         flagged_data, remaining_data = check_deposits_within_time_window(data, time_window)

#         excel_file = generate_excel_report(flagged_data)
#         st.download_button(label="Download Report as Excel", data=open(excel_file, 'rb').read(), file_name="report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

#         st.subheader("Flagged Transactions")
#         st.dataframe(flagged_data)

#         st.subheader("Remaining Transactions")
#         st.dataframe(remaining_data)

#         st.subheader("Statistics")
#         total_flagged_transactions = len(flagged_data)
#         st.write(f"Total Flagged Transactions: {total_flagged_transactions}")

#         transaction_types_stats = flagged_data['txn_type'].value_counts().reset_index()
#         transaction_types_stats.columns = ['Transaction Type', 'Count']
#         transaction_types_stats['Percentage'] = (transaction_types_stats['Count'] / total_flagged_transactions) * 100

#         st.write("Statistics for Flagged Transactions by Transaction Type:")
#         st.dataframe(transaction_types_stats)

# if __name__ == "__main__":
#     main()

import streamlit as st
import dask.dataframe as dd
import pandas as pd

def check_deposits_within_time_window(data, time_window):
    st.spinner("Filtering transactions...")
    
    # Step 1: Filter Transactions
    deposits_data = data[data['txn_type'].isin(['FLOAT_DEPOSIT', 'CDP'])]
    
    st.spinner("Converting date-time...")
    
    # Step 2: Convert Date-Time
    deposits_data['date_time'] = dd.to_datetime(deposits_data['date_time'])
    deposits_data = deposits_data.compute()
    
    st.spinner("Sorting transactions...")
    
    # Step 3: Sort Transactions
    deposits_data = deposits_data.sort_values(['agent_code', 'date_time'])
    
    st.spinner("Calculating time differences...")
    
    # Step 4: Calculate Time Differences
    deposits_data['time_diff'] = deposits_data.groupby('agent_code')['date_time'].diff().dt.total_seconds() / 60
    
    st.spinner("Flagging transactions...")
    
    # Step 5: Flagged Transactions
    flagged_transactions = deposits_data['time_diff'] <= time_window
    flagged_data = deposits_data[flagged_transactions]
    
    # Step 6: Separate Data
    remaining_data = deposits_data[~flagged_transactions]
    
    return flagged_data, remaining_data

def main():
    st.title("Transaction Rule Engine")
    
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
    time_window = st.slider("Time Window (minutes)", min_value=0, max_value=5, value=5)

    if uploaded_file:
        with st.spinner("Reading uploaded file..."):
            if uploaded_file.name.endswith('.csv'):
                data = dd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith('.xlsx'):
                data = pd.read_excel(uploaded_file)
                data = dd.from_pandas(data, npartitions=12)

        with st.spinner("Processing data..."):
            flagged_data, remaining_data = check_deposits_within_time_window(data, time_window)
        
        st.subheader("Flagged Transactions")
        
        # Display flagged transactions
        st.write(flagged_data)

        st.subheader("Remaining Transactions")
        
        # Display remaining transactions
        st.write(remaining_data)
        
        # Download buttons for flagged and remaining data
        st.download_button(label="Download Flagged Data as CSV", data=flagged_data.to_csv(index=False), file_name="flagged_data.csv", mime="text/csv")
        st.download_button(label="Download Remaining Data as CSV", data=remaining_data.to_csv(index=False), file_name="remaining_data.csv", mime="text/csv")

if __name__ == "__main__":
    main()
