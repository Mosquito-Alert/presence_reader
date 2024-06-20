import pandas as pd
import geopandas as gpd
import csv
import datetime
import psycopg2
import config
import logging
import os

local_path = os.path.dirname(os.path.realpath(__file__))
now = datetime.datetime.now()
date_and_time = now.strftime('%d_%m_%Y__%H_%M_%S')
logfile = local_path + '/presence_reader_{0}.log'.format(date_and_time)
logging.basicConfig(filename=logfile, filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=logging.INFO)


def init_nat_to_munic_id():
    data = {}
    with open(local_path + '/main_municipalitiesnatcode.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader, None)  # skip the headers
        for row in csv_reader:
            data[row[1]] = row[0]
    return data


def generate_update_queries_for( mosquito_species_slug, year, dataframe, nat_to_muni_table, db_connection ):
    cursor = db_connection.cursor()
    rows = 0
    for index, row in dataframe.loc[ (dataframe['cntryCode'] == 'ES') & (dataframe['codeLevel'] == 5) & ( (dataframe[mosquito_species_slug] == 'reported') | (dataframe[mosquito_species_slug] == 'introduced') ) ].iterrows():
        muni_natcode = row['locCode'].split('_')[1]
        muni_id = nat_to_muni_table[muni_natcode]
        cursor.execute(
            """UPDATE main_natcodepresence set ma=True where natmunicipality_id=%s and mosquito_class=%s and year=%s;""",
            (muni_id, mosquito_species_slug, year)
        )
        rows = rows + 1
    logging.info("Updated {0} rows".format(rows))
    db_connection.commit()


def print_config():
    logging.info("Using config file")
    for k in config.params:
        if k == 'db_password':
            logging.info("{0} - {1}".format(k,'********'))
        else:
            logging.info("{0} - {1}".format(k, config.params[k]))
    logging.info("\n")


def main():
    print_config()

    shapefile_dbf = config.params['shapefile_dbf']
    logging.info("Reading dbf file...")
    table = gpd.read_file(shapefile_dbf)
    logging.info("Done reading dbf file\n")
    dataframe = pd.DataFrame(table)

    nat_to_muni_table = init_nat_to_munic_id()
    today = datetime.date.today()
    current_year = today.year

    conn = psycopg2.connect(
        dbname=config.params['db_name'],
        user=config.params['db_user'],
        password=config.params['db_password'],
        port=config.params['db_port'],
        host=config.params['db_host']
    )

    logging.info("Updating albopictus presence...")
    generate_update_queries_for('albopictus', current_year, dataframe, nat_to_muni_table, conn)
    logging.info("Done updating albopictus presence\n")
    logging.info("Updating japonicus presence...")
    generate_update_queries_for('japonicus', current_year, dataframe, nat_to_muni_table, conn)
    logging.info("Done updating japonicus presence\n")

    conn.close()
    # filename = 'status_2303.dbf'
    # table = gpd.read_file(filename)
    # pandas_table = pd.DataFrame(table)
    # keys = list(table.keys())
    # print(keys)
    # print(pandas_table['albopictus'].unique())
    # for index, row in pandas_table.loc[ (pandas_table['cntryCode'] == 'ES') & (pandas_table['codeLevel'] == 5) & ( (pandas_table['albopictus'] == 'reported') | (pandas_table['albopictus'] == 'introduced') ) ].iterrows():
    #     #for index, row in pandas_table.iterrows():
    #     #print(row['cntryCode'], row['cntryName'], row['codeLevel'],row['locCode'],row['locName'],row['aegypti'],row['albopictus'],row['japonicus'],row['koreicus'],row['culex'])
    #     #print(row['cntryCode'], row['cntryName'], row['codeLevel'], row['locCode'], row['locName'], row['aegypti'], row['albopictus'], row['japonicus'], row['koreicus'], row['culex'])
    #     print(row['cntryCode'], row['cntryName'], row['codeLevel'], row['locCode'], row['locName'], row['albopictus'])


if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
