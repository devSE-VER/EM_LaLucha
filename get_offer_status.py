# get_offer_status
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from shutil import move
import constants as c
from process.functions import get_files
from constants import SERVERS, DRV

def main():

    pattern_str = '^EM_TierraMojada-offers_status-(.*)\d{8}(.*)'
    # EM_LaLucha-offers_status-yyyymmdd_HHMM
    files = get_files(c.DIR_OFFER, pattern_str )

    cols = ['id_oferta_cenace',	'starts_dt', 'ends_dt',	'reception_dt',	'processing_dt',
            'offer_made', 'reception_type', 'id_unit', 'offer_status', 'issuing']
    # df_status = pd.DataFrame(columns=cols)
    for file in files:

        print(file)
        # Open file
        path_file = f'{c.DIR_OFFER}\\{file}'
        df_status = pd.read_excel(path_file, skiprows=2, names=cols, usecols='A:J')
        # Validate that row

        # Using Date function
        df_status['starts_dt'] = df_status["starts_dt"].dt.strftime('%Y-%m-%d')
        df_status['ends_dt'] = df_status["ends_dt"].dt.strftime('%Y-%m-%d')

        df_status['reception_dt'] = df_status['reception_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_status['processing_dt'] = df_status['processing_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(df_status)

        if not df_status.empty:
            print('Dataframe with data...')
            # Recorer dataframe

            qry_str = f"insert into ofertas.status select * from (values {','.join([str(i) for i in list(df_status.to_records(index=False))])} "
            qry_str += f" ) as st (id_oferta_cenace, starts_dt, ends_dt, reception_dt, processing_dt, offer_made, "
            qry_str += f" reception_type, id_unit, offer_status, issuing) where not exists (select id_oferta_cenace "
            qry_str += f" from ofertas.status o where st.id_oferta_cenace = o.id_oferta_cenace ) """
            print(qry_str)

            for server, con_data in SERVERS.items():

                conn_str = f"mssql+pyodbc://{con_data['USER']}:{quote_plus(con_data['PW'])}@{con_data['SERVER']}/{con_data['DB']}?driver={DRV}"
                engine = create_engine(conn_str, echo=False)

                if server != 'SAAVI':
                    with engine.connect() as conn:
                        result = conn.execute(text(qry_str))
                        conn.commit()

            # row_data = [str(i) for i in list(df_status.to_records(index=False))]
            # print(row_data)
        # Move file
        move(path_file, f'{c.DIR_OFFER}\\update_offer_header\\{file}')


if __name__ == '__main__':
    main()
