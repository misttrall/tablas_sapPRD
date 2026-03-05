import json
from sqlalchemy import create_engine


def get_engine():

    with open("config.json") as config_file:
        config = json.load(config_file)

    db = config["database"]

    conn_str = (
        f"mssql+pyodbc://{db['user']}:{db['password']}@"
        f"{db['server']}/{db['database']}?"
        "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )

    return create_engine(conn_str)