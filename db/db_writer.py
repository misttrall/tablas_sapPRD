from database.connection import get_engine


def save_to_database(df, table):

    engine = get_engine()

    table_name = f"{table}_Data"

    with engine.begin() as conn:

        df.head(0).to_sql(table_name, conn, if_exists="append", index=False)

        df.to_sql(table_name, conn, if_exists="append", index=False)

    print(f"{len(df)} registros insertados en {table_name}")