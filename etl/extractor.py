import pandas as pd

class DataExtractor:
    """Clase para extraer datos de archivos CSV"""
    
    @staticmethod
    def extract_data(file_path):
        """Extrae datos de un archivo CSV en un DataFrame de pandas"""
        try:
            df = pd.read_csv(file_path)
            print(f"Extracción de '{file_path}' exitosa.")
            return df
        except FileNotFoundError:
            print(f"Error: El archivo '{file_path}' no se encontró.")
            return None
        except pd.errors.ParserError as e:
            print(f"Error de parsing en el archivo '{file_path}': {e}")
            return None