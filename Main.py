
import streamlit as st
# from CSV_Agent import chatbot

from DataVisualisation import GetVisuals
from RulesEngine import RulesEngine
from SequentialRulesEngine import SequentialRulesEngine

def intro():
    import streamlit as st

    st.write("# Welcome  👋")
    st.sidebar.success("Select an Action")

    st.markdown(
        """
        **Use Case:**
      
        **👈 Select an action from the dropdown on the left** .
        """
    )

page_names_to_funcs = {
    "—": intro,
    "Visualise Data": GetVisuals,
    "RulesEngine": RulesEngine,
    "SequentialRulesEngine":SequentialRulesEngine
    # "CSV Chatbot" :chatbot
    
}

demo_name = st.sidebar.selectbox("Choose a Action", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()