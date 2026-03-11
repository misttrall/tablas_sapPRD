etl_state = {
    "running": False,
    "current_table": None,
    "step": None,
    "progress": {},      # cantidad de registros por tabla
    "percentage": 0,
    "last_execution": None,
    "logs": []           # ← ESTA FALTABA
}

def log(msg):

    print(msg)

    etl_state["logs"].append(msg)

    # evitar que crezca infinito
    if len(etl_state["logs"]) > 200:
        etl_state["logs"].pop(0)