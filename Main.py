
import streamlit as st
from BackupCode import main_ui
from rule1 import main_ui2

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

    "BETTER " : main_ui,
    "IN PROGRESS":main_ui2
    
 
    # "RulesEngine": RulesEngine,
    # "SequentialRulesEngine":SequentialRulesEngine
    # "CSV Chatbot" :chatbot
    
}

demo_name = st.sidebar.selectbox("Choose a Action", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()