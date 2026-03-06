# etl_state.py
def log(msg):
    print(msg)  # puedes reemplazar con logging si quieres

etl_state = {
    "running": False,
    "current_table": None,
    "step": None,
    "progress": {},   # cantidad de registros por tabla
    "percentage": 0
}