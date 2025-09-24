import pyodbc

class DataLoader:
    """Clase para cargar datos a la base de datos"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def clean_tables(self):
        """Elimina todos los datos de las tablas en el orden correcto para respetar las FK"""
        cursor = self.db_connection.get_connection().cursor()
        try:
            tables = ['order_details', 'orders', 'customers', 'products', 'FuenteDeDatos']
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
            self.db_connection.get_connection().commit()
            print("Tablas limpiadas exitosamente con DELETE.")
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error al limpiar tablas: {sqlstate}")
            self.db_connection.get_connection().rollback()
        finally:
            cursor.close()
    
    def get_valid_ids(self, table_name, id_column):
        """Consulta la base de datos para obtener los IDs que realmente existen en una tabla"""
        cursor = self.db_connection.get_connection().cursor()
        try:
            query = f"SELECT {id_column} FROM {table_name}"
            cursor.execute(query)
            ids = {row[0] for row in cursor.fetchall()}
            print(f"IDs válidos de '{table_name}' obtenidos de la base de datos.")
            return ids
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error al obtener IDs de '{table_name}': {sqlstate}")
            return set()
        finally:
            cursor.close()
    
    def load_data(self, df, table_name):
        """Carga un DataFrame de pandas en una tabla de SQL Server"""
        if df is None or df.empty:
            print(f"No hay datos para cargar en la tabla '{table_name}'.")
            return
        
        cursor = self.db_connection.get_connection().cursor()
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?' for _ in df.columns])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        successful_inserts = 0
        for row in df.itertuples(index=False, name=None):
            try:
                cursor.execute(insert_query, row)
                successful_inserts += 1
            except pyodbc.Error as ex:
                continue
        
        self.db_connection.get_connection().commit()
        cursor.close()
        print(f"Carga de datos en la tabla '{table_name}' exitosa. {successful_inserts} registros insertados.")