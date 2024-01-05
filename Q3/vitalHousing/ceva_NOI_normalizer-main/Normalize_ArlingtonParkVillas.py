import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv('.env')

excel_file_name = 'C:/xampp/htdocs/RentInsite/Q3/vitalHousing/ceva_NOI_normalizer-main/11.2023 Arlington Park T12.xlsx'
print("Excel file name:", excel_file_name)  # Debugging line

df = pd.read_excel(excel_file_name, nrows=5)

is_12_month_trailing_income_statement = df.iloc[:,0].str.contains('Twelve Month Trailing Income Statement').any()
is_trailing_profit_and_loss_detail = df.iloc[:,0].str.contains('Trailing Profit And Loss Detail').any()		


if is_trailing_profit_and_loss_detail:
    import _Normalize_ArlingtonParkVillas_trailing_profit_and_loss_detail
elif is_12_month_trailing_income_statement:
    import _Normalize_ArlingtonParkVillas_12_month_trailing_income_statement
else:
    raise ValueError("Report type not supported")
