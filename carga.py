from prefect import task
from credenciales import conexion_proyectobd

@task
def cargar_Datos(df_limpio):
    print("\n💾 CARGA – Enviando datos a MySQL...")

    con = conexion_proyectobd()
    df_limpio.to_sql(
        name = "bd_proyecto_final",
        con = con,
        if_exists="replace",  # crea la tabla limpia cada ejecucion
        index=False
    )

    print("✔ Datos cargados correctamente en bd_proyecto_final")