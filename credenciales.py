import sqlalchemy  


def conexion_proyectobd():
    username = 'root'
    hostname = '127.0.0.1'
    database = 'base_proyecto_final'
    password = 'Arelyco509*'
    con = sqlalchemy.create_engine(f'mysql+mysqlconnector://{username}:{password}@{hostname}/{database}')
    return con