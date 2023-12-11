import calendar
from datetime import datetime
import utils
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv('.env')

excel_file_name = os.getenv('excel_file')
output_sheet_name = "transformed_data"


def parse_date(date_string):
    parsed_date = datetime.strptime(date_string, "%b %Y")
    last_day_of_month = calendar.monthrange(parsed_date.year, parsed_date.month)[1]
    return datetime(parsed_date.year, parsed_date.month, last_day_of_month).date().strftime("%m/%d/%Y")


try:
    conn = utils.connect_to_database()
    cursor = conn.cursor()
    deal_name = " ".join(excel_file_name.split(" ")[:-2])
    cursor.execute(f"SELECT DealID FROM UW_Deals WHERE DealName='{deal_name}'")
    results = cursor.fetchone()

    if results:
        print("DealID : ",results[0])

        deal_id = results[0]

        df = pd.read_excel(excel_file_name)
        
        index = df.index[(df["Arlington Park Villas"] == "NET OPERATING INCOME") | (
                    df["Arlington Park Villas"] == "Net Operating Income")].tolist()

        df1 = df.iloc[5:index[0], 3:16]

        df1.columns = list(df.iloc[4,3:16])

        df1 = df1.dropna()
        
        df1.columns.values[0] = "CodeName"

        melted_df = df1.melt(id_vars="CodeName", var_name='Date', value_name='Amount')
        
        melted_df["CodeName"] = melted_df["CodeName"].str.extract(r'\s(.*)')

        melted_df["Date"] = melted_df["Date"].str.replace(" Actual","")
        
        melted_df["Date"] = melted_df["Date"].apply(lambda x: parse_date(x))

        melted_df["DealID"] = deal_id
        
        df_out = melted_df[["DealID","Date","CodeName","Amount"]]

        df_out_excel = df_out[["Date","CodeName","Amount"]]

        df_out_excel.loc[:, "Amount"] = df_out_excel["Amount"].astype(int)

        utils.load_data_in_database(conn, deal_id, df_out)
        with pd.ExcelWriter(excel_file_name, engine='openpyxl', mode='a') as writer:
            # Write the DataFrame to the existing Excel file with a new sheet name
            df_out_excel.to_excel(writer, sheet_name=output_sheet_name, index=False)
    else:
        print(f"Error invalid DealName: No deal name found in Database '{deal_name}'")


except Exception as e:
    print(e)