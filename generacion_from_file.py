# Import generation from file send by Gustavo

import pandas as pd
import re
from openpyxl import load_workbook
from sqlalchemy import create_engine
from os import scandir
from shutil import move
from urllib.parse import quote_plus
from constants import DIR_GEN, SERVERS, DRV


def get_files(path_gen, regex_str):
    return [obj.name for obj in scandir(path_gen)
            if obj.is_file() and re.search(regex_str, obj.name) and obj.name.endswith('.xlsx')]


def imp_generation(srv, df_data):
    con_data = srv
    conn_str = f"mssql+pyodbc://{con_data['USER']}:{quote_plus(con_data['PW'])}@{con_data['SERVER']}/{con_data['DB']}?driver={DRV}"
    engine = create_engine(conn_str, echo=False)

    df_data.to_sql(name='Generacion', con=engine, if_exists='append', chunksize=1000,
                   schema='dbo', index=False)


def main():
    print(DIR_GEN)
    # LOAD PROFILE MAYO_LA LUCHA 2023_06062023.xlsx
    pattern_file = '^LOAD PROFILE LA LUCHA(.*)'
    file_list = get_files(DIR_GEN, pattern_file)
    if file_list:
        print(file_list)
        # Process file
        for file in file_list:
            # Get date
            # The regex pattern that we created
            pattern = '\d{8}'
            # Will return all the strings that are matched
            date_search = re.search(pattern, file)
            print(date_search)
            date_str = date_search.group()
            opr_day = date_str[:2]
            opr_month = date_str[2:4]
            opr_year = date_str[4:8]
            opr_dt = f'{opr_year}-{opr_month}-{opr_day}'

            # TODO: Validate that doesn't exist data for opr_dt into the table

            print(date_str)

            month_str = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
                         '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'NOV', '12': 'Dec'}

            path_file = f'{DIR_GEN}\\{file}'
            path_loaded = f'{DIR_GEN}\\loaded\\{file}'
            sheet_name = f"{opr_day}-{month_str[opr_month]}"

            wb = load_workbook(path_file, data_only=True)
            print(wb.sheetnames)

            ws = wb[sheet_name]

            gen_matrix = []
            for i in range(3, 27):
                opr_hr = ws[f"I{i}"].value
                gen_mw = round(ws[f"K{i}"].value,2)
                data = [opr_dt, opr_hr, gen_mw]

                gen_matrix.append(data)

            print(gen_matrix)
            cols = ['opr_dt', 'opr_hr', 'generacion_mw']
            df_gen = pd.DataFrame(data=gen_matrix, columns=cols)

            print(df_gen)

            # Import to DB
            if not df_gen.empty:
                # Seguro SQL Server
                imp_generation(SERVERS['SE'], df_gen)

                # Saavi SQL Server
                imp_generation(SERVERS['SAAVI'], df_gen)

                # Move file to loaded folder
                move(path_file, path_loaded)


if __name__ == '__main__':
    main()
