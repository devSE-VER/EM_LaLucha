# Import generation from file send by Gustavo

import pandas as pd
import re
from openpyxl import load_workbook
from constants import DIR_GEN
from process.functions import get_files
from process.database import check_data_exist, get_validated_engine
from os import getenv
from dotenv import load_dotenv


def process_files(engine):
    # LOAD PROFILE MAYO_LA LUCHA 2023_06062023.xlsx
    pattern_file = '^LOAD PROFILE LA LUCHA(.*).xlsx'
    file_list = get_files(DIR_GEN, pattern_file)

    for file in file_list:
        print(f"\n{file=}")
        # Get date
        # The regex pattern that we created
        pattern = '(\d{2})(\d{2})(\d{4})'
        opr_day, opr_month, opr_year = re.search(pattern, file).groups()
        opr_dt = f'{opr_year}-{opr_month}-{opr_day}'
        # print(f"{opr_dt=}")

        # Validate that doesn't exist data for opr_dt
        if check_data_exist(engine, schema='dbo', table='Generacion', opr_dt=opr_dt):
            print("Alredy loaded!")
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
        try:
            # df_gen.to_sql(name='Generacion', con=eng['engine'], if_exists='append', chunksize=1000, schema='dbo', index=False)
            print("UPLOADED!")
        except Exception as e:
            print(f"Error trying to upload data:", e)


if __name__ == '__main__':
    load_dotenv()
    
    SERVER_SE = {
        'SERVER': getenv('SERVER_SE'),
        'USER': getenv('USER_SE'),
        'PW': getenv('PW_SE')
    }

    SERVER_SAAVI = {
        'SERVER': getenv('SERVER_SAAVI'),
        'USER': getenv('USER_SAAVI'),
        'PW': getenv('PW_SAAVI'),
        'DRV': getenv('SQL_DRIVER'),
    }

    PARTICIPANTS = {
        'G090': { # Los Ramones
            'account': 'EM_LosRamones',
            'servers':{
                'SRV_SE': {'DB': 'EM_LosRamones', 'ENABLE': True, **SERVER_SE}
            },
        },

        'G107': { # La Lucha
            'account': 'EM_LaLucha',
            'servers':{
                'SRV_SE': {'DB': 'EM_LaLucha', 'ENABLE': True, **SERVER_SE},
                'SRV_SAAVI': {'DB': 'BD_Seguro', 'ENABLE': True, **SERVER_SAAVI},
            },
        },

        'G075|C045': { # Tierra Mojada
            'account': 'EM_TierraMojada',
            'servers':{
                'SRV_SE': {'DB': 'EM_TierraMojada', 'ENABLE': True, **SERVER_SE},
                'SRV_SAAVI': {'DB': 'BD_Seguro', 'ENABLE': True, **SERVER_SAAVI},
            },
        }
    }

    #
    for pm, pm_data in PARTICIPANTS.items():
        if pm != 'G107':
            continue
        
        print(f"\n------------{pm=}------------")
        for srv, srv_data in pm_data['servers'].items():
            if srv_data['ENABLE']:
                engine = get_validated_engine(srv_data)
                print(f"{srv=}...", "Ok!" if engine else "Failed!")
                
                if engine:
                    process_files(engine)
