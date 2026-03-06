from extractor.sap_extractor import extract_table
from db.staging_loader import load_staging
from db.merge_runner import run_merges
from db.db_connection import get_engine
from web.utils.etl_state import etl_state, log

import json
import time

def run_bodega_job():
    engine = get_engine()

    with open("config.json") as f:
        config = json.load(f)

    tables = config["tables"]

    # reset estado ETL
    etl_state["running"] = True
    etl_state["current_table"] = None
    etl_state["step"] = "starting"
    etl_state["progress"] = {}
    etl_state["percentage"] = 0

    total_tables = len(tables)
    processed_tables = 0

    try:
        for table in tables:
            source = table["source"]
            target = table["target"]
            fields = config["fields"][source]

            etl_state["current_table"] = source
            etl_state["step"] = "extract"
            log(f"Extrayendo {source}...")

            df = extract_table(source, fields)
            count = len(df)
            log(f"{count} registros extraídos de {source}")

            # Actualizar progreso parcial
            etl_state["progress"][source] = count
            etl_state["percentage"] = int((processed_tables / total_tables) * 80)

            # Cargar en staging
            etl_state["step"] = "staging"
            load_staging(df, target, engine)
            log(f"{source} cargado en staging")

            processed_tables += 1
            etl_state["percentage"] = int((processed_tables / total_tables) * 80)

        # MERGE final
        etl_state["step"] = "merge"
        log("Ejecutando MERGES...")
        run_merges()
        etl_state["percentage"] = 100
        log("ETL finalizado correctamente")

    except Exception as e:
        log(f"ERROR ETL: {str(e)}")
        etl_state["step"] = "error"
        raise

    finally:
        etl_state["running"] = False
        etl_state["current_table"] = None