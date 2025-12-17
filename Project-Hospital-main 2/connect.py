#-------------------------------------------------------------------------------------------------------
#CONEXION MONGODB

from pymongo import MongoClient
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBConnection:
    def __init__(self, connection_string="mongodb://localhost:27017/"):
        self.connection_string = connection_string
        self.client = None
        self.db = None
        
    def connect(self):
        """Establece conexión con MongoDB"""
        self.client = MongoClient(
            self.connection_string,
            serverSelectionTimeoutMS=5000  # 5 segundos timeout
        )
            
        # Probar conexión
        self.client.admin.command('ping')
        self.db = self.client['Hospital']
        logger.info("✅ Conexión exitosa a MongoDB")
        return True
    
    def close(self):
        """Cierra la conexión"""
        if self.client:
            self.client.close()
            logger.info(" Conexión cerrada")
    
    def get_db(self):
        """Retorna la base de datos"""
        return self.db

#-------------------------------------------------------------------------------------------------------
#CONEXION DE DGRAPH

# DGraph/connect.py
import pydgraph

# Crear y retornar un cliente Dgraph
def create_client():
    client_stub = pydgraph.DgraphClientStub("localhost:9080") # Ajusta la dirección y el puerto según tu configuración
    return pydgraph.DgraphClient(client_stub)
