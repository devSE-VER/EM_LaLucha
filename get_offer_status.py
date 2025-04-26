# get_offer_status
import pandas as pd
from sqlalchemy import text
import constants as c
from dotenv import load_dotenv
from os import getenv

from process.functions import get_files
from process.database import get_validated_engine


def process_offers(account:str, engine):
    
    # EM_LaLucha-offers_status-yyyymmdd_HHMM
    pattern_str = fr'^{account}-offers_status-(.*)\d{{8}}(.*)\.xlsx'
    files = get_files(c.DIR_OFFER, pattern_str)

    cols = ['id_oferta_cenace',	'starts_dt', 'ends_dt',	'reception_dt',	'processing_dt',
            'offer_made', 'reception_type', 'id_unit', 'offer_status', 'issuing']
    # df_status = pd.DataFrame(columns=cols)

    for file in files:
        print(f"{file=}", end='... ')
        
        # Open file
        path_file = f'{c.DIR_OFFER}\\{file}'
        df_status = pd.read_excel(path_file, skiprows=2, names=cols, usecols='A:J')
        # Validate that row

        # Using Date function
        df_status['starts_dt'] = df_status["starts_dt"].dt.strftime('%Y-%m-%d')
        df_status['ends_dt'] = df_status["ends_dt"].dt.strftime('%Y-%m-%d')

        df_status['reception_dt'] = df_status['reception_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_status['processing_dt'] = df_status['processing_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')

        if df_status.empty:
            continue

        # print('Dataframe with data...')
        # print(df_status)

        qry_str = f"insert into ofertas.status select * from (values {','.join([str(i) for i in list(df_status.to_records(index=False))])} "
        qry_str += f" ) as st (id_oferta_cenace, starts_dt, ends_dt, reception_dt, processing_dt, offer_made, "
        qry_str += f" reception_type, id_unit, offer_status, issuing) where not exists (select id_oferta_cenace "
        qry_str += f" from ofertas.status o where st.id_oferta_cenace = o.id_oferta_cenace ) """
        # print(qry_str)

        # qry_str = "SELECT 1" # test

        try:
            with engine.connect() as conn:
                result = conn.execute(text(qry_str))
                conn.commit()
            print(f"Uploaded!")
        
        except Exception as e:
            print(f"Insert error: {e}")
            break



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
                'SRV_SAAVI': {'DB': 'BD_Seguro', 'ENABLE': False, **SERVER_SAAVI},
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
        print(f"\n------------{pm=}------------")

        for srv, srv_data in pm_data['servers'].items():
            if srv_data['ENABLE']:
                engine = get_validated_engine(srv_data)
                print(f"{srv=}...", "Ok!" if engine else "Failed!")
                
                if engine:
                    process_offers(pm_data['account'], engine)
