from database.connection import get_engine
from sqlalchemy import text
from datetime import datetime


def log_start(table):

    engine = get_engine()

    with engine.begin() as conn:

        conn.execute(text("""
        MERGE etl_control AS target
        USING (SELECT :table AS table_name) AS src
        ON target.table_name = src.table_name
        WHEN MATCHED THEN
            UPDATE SET status='RUNNING', last_run=GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (table_name,status,last_run)
            VALUES (:table,'RUNNING',GETDATE());
        """), {"table": table})


def log_finish(table, rows, duration, error=None):

    engine = get_engine()

    with engine.begin() as conn:

        conn.execute(text("""
        UPDATE etl_control
        SET status=:status,
            rows_loaded=:rows,
            duration_seconds=:duration,
            error_message=:error
        WHERE table_name=:table
        """), {
            "status": "ERROR" if error else "SUCCESS",
            "rows": rows,
            "duration": duration,
            "error": error,
            "table": table
        })