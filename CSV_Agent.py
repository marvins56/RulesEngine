from langchain.agents import create_csv_agent
from langchain.llms import OpenAI
from langchain.chat_models import ChatOpenAI
from langchain.agents.agent_types import AgentType
import os
import pandas as pd
from dotenv import find_dotenv, load_dotenv
import streamlit as st
import time

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def load_file():
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx"])
    if uploaded_file is not None:
        file_type = uploaded_file.type
        file_path = None

        if file_type == "text/csv":
            file_path = save_file(uploaded_file, "csv")
        elif file_type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.ms-excel"]:
            file_path = save_file(uploaded_file, "xlsx")
            file_path = convert_to_csv(file_path)
        else:
            st.error("Unsupported file type")
        
        return file_path
    else:
        st.error("Please upload a valid CSV or Excel file.")
        return None

def save_file(uploaded_file, file_extension):
    with st.spinner("Saving the file..."):
        file_path = f"{uploaded_file.name.replace(' ', '_').lower()}.{file_extension}"
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        return file_path

def convert_to_csv(excel_file_path):
    csv_file_path = excel_file_path.replace(".xlsx", ".csv")
    excel_data = pd.read_excel(excel_file_path)
    excel_data.to_csv(csv_file_path, index=False)
    return csv_file_path

def chatbot():
    csv_file_path = load_file()

    # Ensure that a valid file path is obtained before proceeding
    if csv_file_path:
        # Create Langchain agent
        agent = create_csv_agent(
            OpenAI(temperature=0),
            csv_file_path,
            verbose=True,
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        )

        # Initialize chat history
        if "messages" not in st.session_state:
            st.session_state.messages = []

        # Display chat messages from history on app rerun
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # Accept user input
        prompt = st.chat_input("Enter your message :")
        
        if prompt:
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": prompt})

            # Display user message in chat message container
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.spinner("Processing..."):
                response = agent.run(prompt)

                # Display assistant response in chat message container
                with st.chat_message("assistant"):
                    message_placeholder = st.empty()
                    full_response = ""
                    assistant_response = response

                    # Simulate stream of response with milliseconds delay
                    for chunk in assistant_response.split():
                        full_response += chunk + " "
                        time.sleep(0.05)
                        # Add a blinking cursor to simulate typing
                        message_placeholder.markdown(full_response + "▌")
                        message_placeholder.markdown(full_response)

                    # Add assistant response to chat history
                    st.session_state.messages.append({"role": "assistant", "content": full_response})
                    
if __name__ == "__main__":

    chatbot()
