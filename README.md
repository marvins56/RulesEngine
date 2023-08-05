# RulesEngine
**Mobile Money & Agent Banking Rules Engine**

The Mobile Money & Agent Banking Rules Engine is a tool designed to analyze transactions data and apply specific rules to identify potential issues or anomalies. It allows users to upload a CSV or Excel file containing transaction data and then select from a list of predefined rules to apply to the data. The tool will flag transactions that meet the criteria specified in the selected rule and display the results in a tabular format.

**Rules**

The following rules are available in the Rules Engine:

Rule: Liquidations from the same number within a specified time window (minutes)
This rule identifies transactions where a customer has made multiple liquidations within a specified time window. It is designed to detect potential fraudulent or suspicious behavior.

**Parameters:**

time_window (int): The time window in minutes within which liquidations are considered as potentially related.
Rule: Deposits or Float purchases to the same account within a specified time window (minutes)
This rule identifies transactions where an agent has made multiple deposits or float purchases to the same account within a specified time window. It can help detect agents who are abusing the system by making frequent deposits or purchases to the same account.

**Parameters:**

time_window (int): The time window in minutes within which deposits or float purchases are considered as potentially related.
agent_column (str): The name of the column containing the agent's information.
account_column (str): The name of the column containing the account information.
date_column (str): The name of the column containing the transaction dates.


**How to Use**

Run the RulesEngine function, and it will prompt you to upload a CSV or Excel file containing transaction data.
The tool will clean the data and display an overview of the dataset.
Select a rule from the dropdown list to inspect transactions based on that rule.
If a rule requires additional parameters, such as the time window, use the provided slider to set the desired value.
The flagged transactions that meet the rule's criteria will be displayed in a table with their corresponding data highlighted in random colors.

**Example**

Suppose we have a dataset containing mobile money transaction records, including customer information, biller details, transaction items, and dates. We can use the Rules Engine to identify potential anomalies or suspicious transactions based on the defined rules.

For instance, by selecting the "Rule: Liquidations from the same number within a specified time window (minutes)" and setting the time window to 10 minutes, the tool will identify transactions where a customer has made multiple liquidations within a 10-minute time frame.

Similarly, by selecting the "Rule: Deposits or Float purchases to the same account within a specified time window (minutes)" and setting the time window to 5 minutes, the tool will flag transactions where an agent has made multiple deposits or float purchases to the same account within a 5-minute time frame.

**Required Package Versions**

The Mobile Money & Agent Banking Rules Engine requires the following package versions:

pandas==1.5.3
seaborn==0.12.2
plotly==5.15.0
numpy==1.23.5
matplotlib==3.7.1
langchain==0.0.212
beautifulsoup4==4.12.2
python-dotenv==1.0.0
