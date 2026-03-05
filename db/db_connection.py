import json
from sqlalchemy import create_engine


def get_engine():

    with open("config.json") as config_file:
        config = json.load(config_file)

    db = config["database"]

    connection_string = (
        f"mssql+pyodbc://{db['user']}:{db['password']}@{db['server']}/{db['database']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&TrustServerCertificate=yes"
    )

    engine = create_engine(connection_string, fast_executemany=True)

    return engine