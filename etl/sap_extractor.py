import json
from pyrfc import Connection
import pandas as pd


def extract_table(table, fields):

    with open("config.json") as config_file:
        config = json.load(config_file)

    sap_conn = Connection(
        user=config['sap']['user'],
        passwd=config['sap']['password'],
        ashost=config['sap']['ashost'],
        sysnr=config['sap']['sysnr'],
        client=config['sap']['client']
    )

    result = sap_conn.call(
        'RFC_READ_TABLE',
        QUERY_TABLE=table,
        DELIMITER='|',
        FIELDS=[{'FIELDNAME': f} for f in fields]
    )

    rows = [row['WA'].split('|') for row in result['DATA']]

    df = pd.DataFrame(rows, columns=fields)

    sap_conn.close()

    return df