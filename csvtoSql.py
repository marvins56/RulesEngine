import streamlit as st
import pandas as pd
import os

def generate_sql_insert(table_name, row):
    columns = ', '.join([f"[{col}]" for col in row.index])
    values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
    return f"INSERT INTO {table_name} ({columns}) VALUES ({values});"

def convert_to_sql_server_script(file, table_name="data_table"):
    file_extension = os.path.splitext(file.name)[-1].lower()
    
    if file_extension == ".csv":
        df = pd.read_csv(file)
    elif file_extension in [".xlsx", ".xls"]:
        df = pd.read_excel(file)
    else:
        st.write("Unsupported file type. Please provide a CSV or Excel file.")
        return
    
    sql_statements = [generate_sql_insert(table_name, row) for _, row in df.iterrows()]
    
    return "\n".join(sql_statements)

st.title('CSV/Excel to SQL Server Script Converter')
uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx', 'xls'])

if uploaded_file:
    table_name = st.text_input("Enter table name for SQL Server:", value="data_table")
    if st.button('Convert'):
        sql_script = convert_to_sql_server_script(uploaded_file, table_name)
        if sql_script:
            st.text_area("SQL Script:", value=sql_script, height=300)
            with open("output.sql", "w") as f:
                f.write(sql_script)
            st.markdown("[Download SQL Script](output.sql)")
