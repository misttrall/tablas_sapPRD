import json
from pyrfc import Connection
import pandas as pd

def extract_table(table, fields, batch_size=30000):
    with open("config.json") as config_file:
        config = json.load(config_file)

    sap_conn = Connection(
        user=config['sap']['user'],
        passwd=config['sap']['password'],
        ashost=config['sap']['ashost'],
        sysnr=config['sap']['sysnr'],
        client=config['sap']['client']
    )

    rows = []
    offset = 0

    while True:
        result = sap_conn.call(
            "RFC_READ_TABLE",
            QUERY_TABLE=table,
            DELIMITER="|",
            FIELDS=[{"FIELDNAME": f} for f in fields],
            ROWCOUNT=batch_size,
            ROWSKIPS=offset
        )

        data = result["DATA"]
        if not data:
            break

        for row in data:
            values = [v.strip() for v in row["WA"].split("|")]
            rows.append(values[:len(fields)])

        print(f"{table}: {len(rows)} registros extraídos (offset {offset})")
        offset += batch_size

    sap_conn.close()

    df = pd.DataFrame(rows, columns=fields)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return df