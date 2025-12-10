from prefect import Flow
from prefect.schedules import IntervalSchedule
from datetime   import timedelta

from extraccion import extract
from transformacion import transform
from carga import cargar_Datos

# Scheduler automático cada 10 segundos
schedule = IntervalSchedule(interval=timedelta(seconds=10))

with Flow("ETL-BaseProyecto", schedule=schedule) as flow:

    data = extract()
    data_limpia = transform(data)
    cargar_Datos(data_limpia)

if __name__== "__main__":
    flow.run()