from etl.sap_extractor import extract_table
from database.db_writer import save_to_database
from database.etl_control import log_start, log_finish
from etl.table_jobs import BODEGA_TABLES
import json
import time


def run_bodega_job():

    with open("config.json") as config_file:
        config = json.load(config_file)

    fields_config = config["fields"]

    for table in BODEGA_TABLES:

        start = time.time()

        try:

            log_start(table)

            fields = fields_config[table]

            print(f"Extrayendo {table}")

            df = extract_table(table, fields)

            save_to_database(df, table)

            duration = int(time.time() - start)

            log_finish(table, len(df), duration)

        except Exception as e:

            log_finish(table, 0, 0, str(e))