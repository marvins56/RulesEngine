import streamlit as st
import pandas as pd
import base64
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from tabulate import tabulate
import os

def process_data_and_generate_pdf(uploaded_file, batch_num):
    datadump = pd.read_excel(uploaded_file, usecols=[0, 1, 2, 3, 7, 8, 12, 14, 15, 22], skiprows = 0)

        # ... (rest of your data processing code, with the batch number integrated) ...
    # Filter out successful transactions for a specific batch number
    response_code_filter = datadump['Response_code'] == 0
    # Convert the 'batch' column to integer without decimals and handle missing values
    datadump['batch'] = datadump['batch'].round(0).fillna(-1).astype(int)

    batch_filter = datadump['batch'] == batch_num
    filtered_datadump = datadump[response_code_filter & batch_filter].copy()

    # Rename columns
    new_column_names = {
        'date_time': 'Tran Date', 'trn_ref': 'Tran Reference', 'batch': 'Batch',
        'Issuer_code': 'Issuer Code', 'Acquirer_code': 'Acquirer Code',
        'agent_code': 'Agent Code', ' Amount ': 'Tran Amount', 'ACC/NO': 'Customer Account',
        'txn_type': 'Tran Type'
    }
    filtered_datadump.rename(columns = new_column_names, inplace = True)

    # Drop unnecessary columns
    columns_to_drop = ['Tran Date', 'Agent Code', 'Customer Account', 'Tran Reference', 'Agent Code', 'Response_code']
    df = filtered_datadump.drop(columns = columns_to_drop, inplace = False)  # Use inplace=False to return a modified copy
    if df.empty:
        st.error("No data found for the specified batch number.")
        return
    else:
        batch_number = df.iloc[0]['Batch']

    # Fill empty fields in "Amount" column with 0
    df['Tran Amount'].fillna(0, inplace = True)

    # Convert 'Trans Amount' column to numeric
    df['Tran Amount'] = pd.to_numeric(df['Tran Amount'], errors ='coerce')

    # Swift Code mapping dictionary
    swift_mapping = {
        230147: 'HFINUGKA',163747: 'CERBUGKA',252947: 'KCBLUGKA',13847: 'BARCUGKX',560147: 'UGPBUGKA',20147: 'BARBUGKA',
        110147: 'ORINUGKA',190147: 'DTKEUGKA',360147: 'CBAFUGKA',80147: 'SCBLUGKA',60147: 'TROAUGKA',320147: 'EXTNUGKA',
        180147: 'CAIEUGKA',260147: 'UNAFUGKA',610147: 'OPUGUGKA',730147: 'CERBUGKA',290147: 'ECOCUGKA',270147: 'GTBIUGKA',
        40147: 'SBICUGKX',300147: 'EQBLUGKA',50147: 'DFCUUGKA',410147: 'FTBLUGKA',130447: 'AFRIUGKA'}

    # Map Issuer Code to Swift Code using the dictionary
    df['Payer'] = df['Acquirer Code'].map(swift_mapping)
    df['Beneficiary'] = df['Issuer Code'].map(swift_mapping)

    # Extract the batch number (assuming it's the same for all rows)
    batch_number = df.iloc[0]['Batch']


    combined_dict = {}

    for index, row in df.iterrows():
        acquirer = row["Payer"]
        issuer = row["Beneficiary"]
        tran_amount = row["Tran Amount"]
        key = (acquirer, issuer)
        
        if acquirer != issuer and row["Tran Type"] not in ["CLF", "CWD"]:
            if key in combined_dict:
                combined_dict[key] += tran_amount
            else:
                combined_dict[key] = tran_amount

        if acquirer != issuer and row["Tran Type"] in ["CLF", "CWD"]:
            if key in combined_dict:
                combined_dict[key] += tran_amount
            else:
                combined_dict[key] = tran_amount

    # Convert combined_dict to DataFrame
    combined_result = pd.DataFrame(combined_dict.items(), columns=["Key", "Amount"])

    # Split the "Key" column into "Acquirer Code" and "Issuer Code" columns
    combined_result[["Payer", "Beneficiary"]] = pd.DataFrame(combined_result["Key"].tolist(), index=combined_result.index)

    # Drop the "Key" column
    combined_result = combined_result.drop(columns=["Key"])

    # Convert "Amount" column to numeric
    combined_result["Amount"] = combined_result["Amount"].astype(float)

    # Remove rows with NaN or zero Amount
    combined_result = combined_result[combined_result["Amount"].notna() & (combined_result["Amount"] != 0)]

    # Calculate total_amount and create total_row

    #combined_result["Payer"] = ["Total", "", f"{total_amount:,.0f}"]

    # Append total_row to the DataFrame
    #combined_result.loc[len(combined_result)] = total_row

    # Convert "Acquirer" column to strings
    combined_result["Payer"] = combined_result["Payer"].astype(str)

    # Order records by Acquirer Code column
    combined_result = combined_result.sort_values(by=["Payer"])

    # # Format and align columns for display
    # print("Combined Table:")
    # print(tabulate(combined_result, headers="keys", tablefmt="grid", showindex=False, numalign="center", stralign="center", colalign=("center", "center", "center")))

    pdf_filename = f"Settlement Report {int(batch_num)}.pdf"
    doc = SimpleDocTemplate(pdf_filename, pagesize=letter)


        
    # Create a list to hold elements for the PDF
    elements = []

    # Add title with batch number
    title_style = getSampleStyleSheet()["Title"]
    title = Paragraph("Settlement Report", title_style)
    elements.append(title)

    # Add batch number
    batch_number_text = f"Batch Number: {batch_number}"
    batch_number_style = getSampleStyleSheet()["Normal"]
    batch_number_paragraph = Paragraph(batch_number_text, batch_number_style)
    elements.append(batch_number_paragraph)

    # Add a spacer
    elements.append(Spacer(1, 20))  # Adds a vertical space of 20 points
    elements.append(Spacer(1, 20))  # Adds a vertical space of 20 points

    # Add subtitle
    subtitle_style = getSampleStyleSheet()["Normal"]
    subtitle = Paragraph("Settled by:", subtitle_style)
    elements.append(subtitle)

    # Create a spacer
    elements.append(Spacer(1, 10))  # Adds a smaller vertical space
    elements.append(Spacer(1, 10))  # Adds a smaller vertical space

    # Create a signature section
    signature_style = getSampleStyleSheet()["Normal"]
    signature_section = Paragraph("Signature: _______________________", signature_style) 
    elements.append(signature_section)

    # Create a spacer
    elements.append(Spacer(1, 20))  # Adds a vertical space of 20 points
    elements.append(Spacer(1, 20))  # Adds a vertical space of 20 points

    # Create a list to hold table data
    table_data = []

    # Add header row to table data
    table_data.append(["Payer", "Beneficiary", "Amount"])

    # Add rows from the combined result to the table data
    for _, row in combined_result.iterrows():
        if row['Payer'] == "Total":
            continue  # Skip the 'Total' row
        Payer = row['Payer']
        Beneficiary = row['Beneficiary']
        Amount = f"{int(row['Amount']):,}"  # format amount, add comma separaters, remove decimals
        table_data.append([Payer, Beneficiary, Amount])

    # Create a Table object
    # Add "Total" row at the end
    total_amount = combined_result['Amount'].sum()
    total_row = ["Total", "", f"{total_amount:,.0f}"]
    table_data.append(total_row)
    table = Table(table_data)

    # Apply TableStyle to format the table
    table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                            ('ALIGN', (0, 0), (-1, 0), 'LEFT'),  # Align Payer and Beneficiary to the left
                            ('ALIGN', (-1, 0), (-1, -1), 'RIGHT'),  # Align Trans Amount to the right
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black)]))

    # Add the table to the elements list
    elements.append(table)

    # Add a spacer
    elements.append(Spacer(1, 20))  # Adds a vertical space of 20 points

    # Add "Signed and approved by" section
    signed_approved_text = "Signed and approved by: Head of Operations"
    signed_approved_style = getSampleStyleSheet()["Normal"]
    signed_approved_section = Paragraph(signed_approved_text, signed_approved_style)
    elements.append(signed_approved_section)

    # Add a spacer
    elements.append(Spacer(1, 20))  # Adds a smaller vertical space
    elements.append(Spacer(1, 20))  # Adds a vertical space of 20 points

    # Add signature and date section
    signature_date = f"Signature: _______________________      Date: {datetime.now().strftime('%Y-%m-%d')}"
    signature_date_style = getSampleStyleSheet()["Normal"]
    signature_date_section = Paragraph(signature_date, signature_date_style)
    elements.append(signature_date_section)



    doc.build(elements)
    
    return pdf_filename

def app_ui():
    st.title("Transaction Report")

    uploaded_file = st.file_uploader("Upload your transaction data", type=["xlsx"])
    
    if uploaded_file:
        batch_num = st.text_input("Enter the batch number (4 digits only):", max_chars=4)
        
        if len(batch_num) == 4 and batch_num.isdigit():
            with st.spinner("Processing data and generating report..."):
                pdf_filename = process_data_and_generate_pdf(uploaded_file, int(batch_num))
                
                if pdf_filename:  # Check if a valid filename was returned
                    st.write(f"PDF saved as {pdf_filename}")

                    # Preview the generated PDF
                    with open(pdf_filename, "rb") as f:
                        base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="400" type="application/pdf"></iframe>'
                        st.markdown(pdf_display, unsafe_allow_html=True)

                        # Provide a download link for the generated PDF
                        href = f'<a href="data:application/pdf;base64,{base64_pdf}" download="{pdf_filename}">Click to download the PDF</a>'
                        st.markdown(href, unsafe_allow_html=True)



if __name__ == "__main__":
    app_ui()
