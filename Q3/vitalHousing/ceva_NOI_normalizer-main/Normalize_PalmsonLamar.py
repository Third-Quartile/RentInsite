import pandas as pd
import utils
from dotenv import load_dotenv
import os
load_dotenv('.env')


excel_file_name = 'C:/xampp/htdocs/RentInsite/Q3/vitalHousing/ceva_NOI_normalizer-main/11.2023 Palms on Lamar T12.xlsx'
output_sheet_name = "transformed_data"


def getcodename(x):
    try:
        return x.split("- ")[-1]
    except:
        return x

try:
    
    conn = utils.connect_to_database()
    cursor = conn.cursor()
    deal_name = "Palms on Lamar"
    print(f"deal_name: {deal_name}, type: {type(deal_name)}")
    cursor.execute(f"SELECT DealID FROM UW_Deals WHERE DealName='{deal_name}'")
    results = cursor.fetchone()

    if results:

        deal_id = results[0]

        df = pd.read_excel(excel_file_name)
        
        index = df[(df.iloc[:, 0].str.strip() == "NET OPERATING INCOME") | (df.iloc[:, 0].str.strip() == "Net Operating Income")].index
        
        df1 = df.iloc[12:index[0], :13]

        df1.columns = list(df.iloc[7, :13])

        df1.columns.values[0] = "CodeName"

        #df1["CodeName"] = df1.apply(lambda row: getcodename(row["CodeName"]) if 'Total' not in row["CodeName"] else row["CodeName"], axis=1)
        df1["CodeName"] = df1.apply(lambda row: getcodename(row["CodeName"]) if isinstance(row["CodeName"], str) and 'Total' not in row["CodeName"] else row["CodeName"], axis=1)

        df1 = df1.dropna()

        melted_df = df1.melt(id_vars="CodeName", var_name='Date', value_name='Amount')
        
        melted_df["Date"] = pd.to_datetime(melted_df["Date"], format="%m/%d/%Y")

        melted_df["Date"] = melted_df["Date"].dt.strftime('%m/%d/%Y')

        melted_df["DealID"] = deal_id

        #df_filter_unnecesary_rows = melted_df[~melted_df['CodeName'].str.startswith(' ')]
        df_filter_unnecesary_rows = melted_df[~melted_df['CodeName'].astype(str).str.startswith(' ')]


        df_out = df_filter_unnecesary_rows[["DealID", "Date", "CodeName", "Amount"]]

        df_out_excel = df_filter_unnecesary_rows[["Date", "CodeName", "Amount"]]

        df_out_excel = df_out_excel.rename(columns={'Amount': 'Period_to_Date'})

        #df_out_excel['CodeName'] = ' ' + df_out_excel['CodeName']
        df_out_excel['CodeName'] = ' ' + df_out_excel['CodeName'].astype(str)


        utils.load_data_in_database(conn, deal_id, df_out)

        with pd.ExcelWriter(excel_file_name, engine='openpyxl', mode='a') as writer:
            # Write the DataFrame to the existing Excel file with a new sheet name
            df_out_excel.to_excel(writer, sheet_name=output_sheet_name, index=False)

    else:
        print(f"Error invalid DealName: No deal name found in Database '{deal_name}'")

except Exception as e:
    print("Error : ",e)