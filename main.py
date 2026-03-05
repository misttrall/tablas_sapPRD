import time
from etl.etl_runner import run_bodega_job

start = time.time()

run_bodega_job()

end = time.time()

print(f"ETL terminado en {round(end-start)} segundos")