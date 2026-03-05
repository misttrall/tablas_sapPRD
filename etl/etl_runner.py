from extractor.sap_extractor import extract_table
from db.staging_loader import load_staging
from db.merge_runner import run_merges
from db.db_connection import get_engine
from web.utils.etl_state import etl_state, log

import json


def run_bodega_job():

    engine = get_engine()

    with open("config.json") as f:
        config = json.load(f)

    tables = config["tables"]

    etl_state["running"] = True
    etl_state["progress"] = {}

    for table in tables:

        source = table["source"]
        target = table["target"]
        fields = config["fields"][source]

        etl_state["current_table"] = source

        log(f"Extrayendo {source}")

        df = extract_table(source, fields)

        count = len(df)

        log(f"{count} registros extraídos de {source}")

        load_staging(df, target, engine)

        etl_state["progress"][source] = count

        log(f"{source} cargado en staging")

    log("Ejecutando MERGES")

    run_merges()

    log("ETL finalizado")

    etl_state["running"] = False