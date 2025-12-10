import pandas as pd
import xml.etree.ElementTree as ET
import glob
import os
from prefect import task



# ============================ RUTAS ============================
ruta_base = r"C:\Users\HP\Documents\CERTUS"
ruta_descargas = os.path.join(os.path.expanduser("~"), "Downloads")

# ============================ EXTRACT ============================
@task
def extract():
    print("\n📌 EXTRACT – Leyendo archivo XLSX...")

    #------BUSCANDO EL ARCHIVO EN LA CARPETA
    file_path = os.path.join(ruta_base, "Base_proyecto.xlsx")

    #---LEYENDO ARCHIVO
    Base_proyecto = pd.read_excel(file_path)
    print(f"Archivo cargado con {Base_proyecto.shape[0]} filas y {Base_proyecto.shape[1]} columnas.")

    #-Renombrar columnas para evitar espacios

    Base_proyecto.columns =(Base_proyecto.columns.str.strip()
                                    .str.replace(" ", "_")
                                    .str.replace("á", "a")
                                    .str.replace("é", "e")
                                    .str.replace("í", "i")
                                    .str.replace("ó", "o")
                                    .str.replace("ú", "u")
                                    )
    

    #------Expoortar el archivo consolidado
    output_file = os.path.join(ruta_descargas, "etl_extract.csv")
    Base_proyecto.to_csv(output_file, index=False, encoding="utf-8")
    
    print(f"📥 Archivo consolidado guardado en: {output_file}")

    return Base_proyecto    