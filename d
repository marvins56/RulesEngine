def filter_repeated_cash_deposits(cash_deposits):
    try:
        # Group cash deposit data by agent and account, and filter by transactions made more than once
        repeated_cash_deposits = cash_deposits.groupby(['agent_code', 'ACC/NO']).filter(lambda x: len(x) > 1)
        
        return repeated_cash_deposits
    except Exception as e:
        st.error("An error occurred while filtering repeated cash deposits.")
        st.write(e)
        return None


        
# def flag_repeated_transactions_within_time_range(data, time_threshold_minutes):
#     try:
#         # Convert date_time column to datetime format
#         data['date_time'] = pd.to_datetime(data['date_time'])

#         # Initialize an empty DataFrame to store flagged transactions
#         flagged_transactions = pd.DataFrame()

#         # Group data by agent and account
#         grouped = data.groupby(['agent_code', 'ACC/NO'])

#         for name, group in grouped:
#             # Sort transactions by date_time
#             group = group.sort_values('date_time')

#             # Calculate time difference between consecutive transactions
#             group['time_diff'] = group['date_time'].diff()

#             # Select transactions within the given time threshold
#             within_time_range = group[group['time_diff'] <= timedelta(minutes=time_threshold_minutes)]

#             # If there are transactions within the time range, flag them
#             if len(within_time_range) > 1:
#                 within_time_range['flagged_reason'] = 'Multiple Transactions in Time Range'
#                 flagged_transactions = flagged_transactions.append(within_time_range)

#         # Drop the time_diff column
#         flagged_transactions.drop(columns=['time_diff'], inplace=True)

#         return flagged_transactions

#     except Exception as e:
#         st.error("An error occurred while flagging repeated transactions within a time range.")
#         st.write(e)
#         return None