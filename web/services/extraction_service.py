import sys
import os

# Permite importar main.py desde la raíz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import main


class ExtractionService:

    @staticmethod
    def run_full_extraction():
        """
        Ejecuta el flujo productivo completo sin modificarlo.
        """
        main.main()
