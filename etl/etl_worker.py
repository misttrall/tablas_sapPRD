import time
import threading
from web.utils.etl_state import etl_state
from web.services.extraction_service import ExtractionService
from datetime import datetime
INTERVAL_MINUTES = 45


def auto_worker():

    while True:

        if not etl_state.get("running"):

            try:

                print("ETL automático iniciado")

                etl_state["running"] = True
                etl_state["step"] = "auto_start"

                ExtractionService.run_full_extraction()

                print("ETL automático terminado")

            except Exception as e:
                print(f"Error ETL automático: {e}")

            finally:
                etl_state["running"] = False
                etl_state["last_execution"] = datetime.now()

        time.sleep(INTERVAL_MINUTES * 60)


def start_auto_etl():

    thread = threading.Thread(target=auto_worker, daemon=True)
    thread.start()