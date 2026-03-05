from etl.etl_runner import run_bodega_job


class ExtractionService:

    @staticmethod
    def run_full_extraction():

        print("Iniciando ETL...")

        run_bodega_job()

        print("ETL finalizado")