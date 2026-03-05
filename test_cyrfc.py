import os
from ctypes import cdll
import sys

# Ruta a tu _cyrfc.pyd
pyd_path = r"C:\Users\administrador.DOM_INVERTEC\AppData\Local\Programs\Python\Python312\Lib\site-packages\pyrfc\_cyrfc.cp312-win_amd64.pyd"

print("Intentando cargar:", pyd_path)

try:
    cdll.LoadLibrary(pyd_path)
    print("✅ _cyrfc.pyd se cargó correctamente. Todas las DLLs están presentes.")
except OSError as e:
    print("❌ Error al cargar _cyrfc.pyd")
    print(e)
    print("\nSugerencia: revisa que las DLLs de SAP (sapnwrfc.dll, icudt57.dll, icuuc57.dll, icuin57.dll) estén en PATH y coincidan con tu arquitectura (x64).")
