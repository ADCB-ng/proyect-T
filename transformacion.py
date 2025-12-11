import pandas as pd
import os
from prefect import task

ruta_descargas = os.path.join(os.path.expanduser("~"), "Downloads")

@task
def transform(Base_proyecto):

    print("\n Iniciando proceso de limpieza y análisis...")

    
    # 1. GUARDAR COPIA ORIGINAL
    
    df_original = Base_proyecto.copy()

    # Archivo reporte
    reporte_path = os.path.join(ruta_descargas, "reporte_calidad_datos.txt")
    reporte = open(reporte_path, "w", encoding="utf-8")
    reporte.write("-- REPORTE DE CALIDAD DE DATOS ---\n\n")

   
    # 2. VALORES FALTANTES
   
    reporte.write("----- VALORES FALTANTES POR COLUMNA -----\n")
    faltantes = df_original.isna().sum()
    reporte.write(str(faltantes) + "\n\n")

    print("\n Valores faltantes detectados:")
    print(faltantes)

    # Imputación de valores faltantes
    numericas = ["Ingreso", "Costo", "margen"]

    for col in numericas:
        df_original[col].fillna(df_original[col].median(), inplace=True)

    columnas_texto = ["Marca","Gama","Tipo_Venta","CanalVenta","CadenaDealer",
                      "Departamento","Canal","SubCanal","Cluster"]

    for col in columnas_texto:
        df_original[col].fillna("desconocido", inplace=True)

    reporte.write("Valores faltantes imputados correctamente.\n\n")

   
    # 3. DUPLICADOS
  
    duplicados = df_original.duplicated().sum()
    reporte.write("----- DUPLICADOS DETECTADOS -----\n")
    reporte.write(f"Duplicados encontrados: {duplicados}\n")

    print(f"\n Duplicados encontrados: {duplicados}")

    df_original = df_original.drop_duplicates()
    reporte.write("Duplicados eliminados correctamente.\n\n")

   
    # 4. TIPOS DE DATOS
    reporte.write("----- TIPOS DE DATOS ANTES DE CONVERSIÓN -----\n")
    reporte.write(str(df_original.dtypes) + "\n\n")

    # Conversión de tipos
    df_original["Periodo"] = df_original["Periodo"].astype(int)
    df_original["Dia"] = df_original["Dia"].astype(int)
    df_original["Ingreso"] = df_original["Ingreso"].astype(float)
    df_original["Costo"] = df_original["Costo"].astype(float)
    df_original["margen"] = df_original["margen"].astype(float)

    reporte.write("----- TIPOS DE DATOS DESPUÉS DE CONVERSIÓN -----\n")
    reporte.write(str(df_original.dtypes) + "\n\n")

    
    # 5. NORMALIZACIÓN DE TEXTO
    for col in columnas_texto:
        df_original[col] = df_original[col].astype(str).str.lower().str.strip()

    reporte.write("Texto normalizado correctamente.\n\n")
    print("\n Textos normalizados")

   
    # 6. REDONDEO
    df_original["Ingreso"] = df_original["Ingreso"].round(2)
    df_original["Costo"] = df_original["Costo"].round(2)
    df_original["margen"] = df_original["margen"].round(2)

   
    # 7. GUARDADO DEL RESULTADO
    archivo_transformado = os.path.join(ruta_descargas, "etl_final.xlsx")
    df_original.to_excel(archivo_transformado, index=False)

    reporte.write(f"Archivo transformado guardado en: {archivo_transformado}\n")
    reporte.close()

    print(f"\n📘 Archivo transformado guardado en: {archivo_transformado}")
    print(f"📄 Reporte de calidad guardado en: {reporte_path}")

    return df_original
