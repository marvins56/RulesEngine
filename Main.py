
import streamlit as st
# from CSV_Agent import chatbot

from DataVisualisation import GetVisuals
from RulesEngine import RulesEngine

def intro():
    import streamlit as st

    st.write("# Welcome  👋")
    st.sidebar.success("Select an Action")

    st.markdown(
        """
        **Use Case:**
      
        **👈 Select an action from the dropdown on the left** to explore the capabilities of RTT.
        """
    )

page_names_to_funcs = {
    "—": intro,
    "Visualise Data": GetVisuals,
    "RulesEngine": RulesEngine
    # "CSV Chatbot" :chatbot
    
}

demo_name = st.sidebar.selectbox("Choose a Action", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()