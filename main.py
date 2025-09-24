from pipeline.etl_pipeline import ETLPipeline

def main():
    """Función principal que ejecuta el pipeline ETL"""
    
    server = 'DESKTOP-91VF8H7\\SQLEXPRESS'
    database = 'AnalisisVentas'
    
    etl_pipeline = ETLPipeline(server, database)
    etl_pipeline.run()

if __name__ == "__main__":
    main()