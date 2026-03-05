etl_state = {
    "running": False,
    "current_table": None,
    "progress": {},
    "logs": []
}


def log(message):

    print(message)

    etl_state["logs"].append(message)

    if len(etl_state["logs"]) > 100:
        etl_state["logs"].pop(0)