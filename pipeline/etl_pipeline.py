import pandas as pd
from database.connection import DatabaseConnection
from etl.extractor import DataExtractor
from etl.transformer import DataTransformer
from etl.loader import DataLoader

class ETLPipeline:
    """Clase principal que orquesta el proceso ETL completo"""
    
    def __init__(self, server, database):
        self.db_connection = DatabaseConnection(server, database)
        self.data_extractor = DataExtractor()
        self.data_transformer = DataTransformer()
        self.data_loader = DataLoader(self.db_connection)
        self.file_paths = {
            'customers': 'customers.csv',
            'products': 'products.csv',
            'orders': 'orders.csv',
            'order_details': 'order_details.csv'
        }
    
    def run(self):
        """Ejecuta el proceso ETL completo"""
        if not self.db_connection.connect():
            return
        
        self.data_loader.clean_tables()
        
        self._load_source_metadata()
        
        self._process_customers()
        self._process_products()
        self._process_orders()
        self._process_order_details()
        
        self.db_connection.disconnect()
        print("Proceso ETL completado exitosamente.")
    
    def _load_source_metadata(self):
        """Carga los metadatos de las fuentes de datos"""
        fuente_datos = {
            'SourceName': ['customers.csv', 'products.csv', 'orders.csv', 'order_details.csv'],
            'SourceDescription': [
                'Datos de clientes del CRM', 
                'Datos de productos del inventario', 
                'Registro de órdenes de venta', 
                'Detalles de los productos por orden'
            ],
            'SourceType': ['CSV', 'CSV', 'CSV', 'CSV'],
            'ConnectionInfo': [
                'Local File System', 
                'Local File System', 
                'Local File System', 
                'Local File System'
            ]
        }
        fuente_df = pd.DataFrame(fuente_datos)
        self.data_loader.load_data(fuente_df, 'FuenteDeDatos')
    
    def _process_customers(self):
        """Procesa los datos de clientes"""
        df = self.data_extractor.extract_data(self.file_paths['customers'])
        if df is not None:
            df = self.data_transformer.transform_data(df)
            self.data_loader.load_data(df, 'customers')
    
    def _process_products(self):
        """Procesa los datos de productos"""
        df = self.data_extractor.extract_data(self.file_paths['products'])
        if df is not None:
            df = self.data_transformer.transform_data(df)
            self.data_loader.load_data(df, 'products')
    
    def _process_orders(self):
        """Procesa los datos de órdenes"""
        df = self.data_extractor.extract_data(self.file_paths['orders'])
        if df is not None:
            df = self.data_transformer.transform_data(df)
            self.data_loader.load_data(df, 'orders')
    
    def _process_order_details(self):
        """Procesa los detalles de órdenes con validación de claves foráneas"""
        df = self.data_extractor.extract_data(self.file_paths['order_details'])
        if df is not None:
            df = self.data_transformer.transform_data(df)
            
            valid_orders_ids = self.data_loader.get_valid_ids('orders', 'OrderID')
            valid_products_ids = self.data_loader.get_valid_ids('products', 'ProductID')
            
            df = self.data_transformer.validate_foreign_keys(df, valid_orders_ids, valid_products_ids)
            
            if df is not None:
                self.data_loader.load_data(df, 'order_details')