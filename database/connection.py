import pyodbc

class DatabaseConnection:
    """Clase para manejar la conexión a la base de datos"""
    
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        self.connection = None
    
    def connect(self):
        """Establece la conexión a la base de datos"""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            print("Conexión a la base de datos exitosa.")
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error al conectar a la base de datos: {sqlstate}")
            return False
    
    def disconnect(self):
        """Cierra la conexión a la base de datos"""
        if self.connection:
            self.connection.close()
            print("Conexión a la base de datos cerrada.")
    
    def get_connection(self):
        """Retorna la conexión activa"""
        return self.connection