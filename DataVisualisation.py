import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import base64
from plotly import graph_objs as go
from RulesEngine import load_data,clean_data


# Function to visualize the data
def visualize_data(data):
    st.subheader("Data Visualization")
    
    # Countplot of transactions by type (Item)
    plt.figure(figsize=(10, 6))
    sns.countplot(data=data, x='Item')
    plt.xticks(rotation=90)
    plt.title("Transaction Count by Type")
    st.pyplot()

    # Line plot of transaction amount over time
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=data, x='Date', y='Amount')
    plt.title("Transaction Amount Over Time")
    st.pyplot()


def GetVisuals():
    st.title("Mobile Money & Agent Banking Rules Engine")
    st.set_option('deprecation.showPyplotGlobalUse', False)
    
    # Upload CSV or Excel file
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])
    if uploaded_file is not None:
        data = load_data(uploaded_file)

        # Clean data
        data = clean_data(data)

        # Data overview
        st.subheader("Data Overview")
        st.write(data.head())

        visualize_data(data)

if __name__ == "__main__":
    GetVisuals()
