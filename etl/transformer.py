import pandas as pd

class DataTransformer:
    """Clase para transformar y limpiar datos"""
    
    @staticmethod
    def transform_data(df):
        """Limpia y transforma los datos"""
        if df is None or df.empty:
            return df
            
        df_clean = df.copy()
        df_clean.drop_duplicates(inplace=True)
        df_clean.dropna(inplace=True)

        if 'OrderDate' in df_clean.columns:
            df_clean['OrderDate'] = pd.to_datetime(df_clean['OrderDate'], errors='coerce')
            df_clean.dropna(subset=['OrderDate'], inplace=True)

        if 'Price' in df_clean.columns:
            df_clean['Price'] = pd.to_numeric(df_clean['Price'], errors='coerce')
            df_clean.dropna(subset=['Price'], inplace=True)
        
        print("Transformación de datos completada.")
        return df_clean
    
    @staticmethod
    def validate_foreign_keys(order_details_df, valid_orders_ids, valid_products_ids):
        """Valida y filtra las claves foráneas en order_details"""
        try:
            order_details_df['OrderID'] = order_details_df['OrderID'].astype(int)
            order_details_df['ProductID'] = order_details_df['ProductID'].astype(int)
            print("Conversión de IDs de 'order_details' a tipo entero exitosa.")
        except ValueError as e:
            print(f"Error en la conversión de IDs: {e}. Algunos IDs no son numéricos.")
            return None
        
        filtered_df = order_details_df[
            order_details_df['OrderID'].isin(valid_orders_ids) &
            order_details_df['ProductID'].isin(valid_products_ids)
        ]
        
        print("Registros de order_details filtrados para respetar las claves foráneas.")
        return filtered_df