# Import generation from file send by Gustavo

import pandas as pd
import re
from openpyxl import load_workbook
from shutil import move
from constants import DIR_GEN, SERVERS, DRV
from process.functions import get_files
from process.database import check_data_exist, get_engines



def main():
    print(DIR_GEN)
    # add DRV to servers data
    for srv, srv_data in SERVERS.items():
        srv_data['DRV'] = DRV
    
    # LOAD PROFILE MAYO_LA LUCHA 2023_06062023.xlsx
    pattern_file = '^LOAD PROFILE LA LUCHA(.*).xlsx'
    file_list = get_files(DIR_GEN, pattern_file)

    for file in file_list:
        print(f"\n\n{file=}")
        # Get date
        # The regex pattern that we created
        pattern = '(\d{2})(\d{2})(\d{4})'
        opr_day, opr_month, opr_year = re.search(pattern, file).groups()
        opr_dt = f'{opr_year}-{opr_month}-{opr_day}'
        # print(f"{opr_dt=}")

        # Validate that doesn't exist data for opr_dt into each server's table
        engines = get_engines(SERVERS)
        for eng in engines:
            try:
                eng['file_exist'] = check_data_exist(eng['engine'], schema='dbo', table='Generacion', opr_dt=opr_dt)
            except Exception as e:
                print(f"Error trying to validate data on the {eng['srv']} server\n", e)
                engines.remove(eng) # remove engine if the connection fails

        if all([eng['file_exist'] for eng in engines]):
            print("Data alredy load on all servers!")
            continue

        # file processing
        month_str = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun',
                        '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'NOV', '12': 'Dec'}

        path_file = f'{DIR_GEN}\\{file}'
        path_loaded = f'{DIR_GEN}\\loaded\\{file}'
        sheet_name = f"{opr_day}-{month_str[opr_month]}"

        wb = load_workbook(path_file, data_only=True)
        ws = wb[sheet_name]

        gen_matrix = []
        for i in range(3, 27):
            opr_hr = ws[f"I{i}"].value
            gen_mw = round(ws[f"K{i}"].value,2)
            data = [opr_dt, opr_hr, gen_mw]

            gen_matrix.append(data)

        
        cols = ['opr_dt', 'opr_hr', 'generacion_mw']
        df_gen = pd.DataFrame(data=gen_matrix, columns=cols)


        if df_gen.empty:
            print("Nothing to upload!")
            continue
        
        # Import to DB
        for eng in engines:
            if not eng['file_exist']:
                try:
                    # print(f"({eng['srv']}) to sql...")
                    df_gen.to_sql(name='Generacion', con=eng['engine'], if_exists='append', chunksize=1000, schema='dbo', index=False)
                    print("UPLOADED!")
                except Exception as e:
                    print(f"Error trying to upload data on the {eng['srv']} server\n", e)

        # If any connection failed previously
        engines = get_engines(SERVERS)
        for eng in engines:
            eng['file_exist'] = False
            try:
                eng['file_exist'] = check_data_exist(eng['engine'], schema='dbo', table='Generacion', opr_dt=opr_dt)
            except:
                pass
        
        # Move file to loaded folder
        if all([eng['file_exist'] for eng in engines]):
            print("(Move)")
            # move(path_file, path_loaded)


if __name__ == '__main__':
    main()
