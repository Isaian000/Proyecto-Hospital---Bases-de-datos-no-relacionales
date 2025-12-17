from connect import MongoDBConnection
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class HospitalModel:
    def __init__(self):
        self.conn = MongoDBConnection()
        self.db = None
    
    def connect(self):
        if self.conn.connect():
            self.db = self.conn.get_db()
            return True
        return False
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    #  PACIENTES

    def get_all_pacientes(self):
        return list(self.db.pacientes.find({}, {
            '_id': 0, 'Paciente_ID': 1, 'Nombre': 1, 'Apellido': 1,
            'Telefono': 1, 'Domicilio.Ciudad': 1, 'Fecha_de_Nacimiento': 1
        }))

    def get_paciente_by_id(self, paciente_id):
        try:
            return self.db.pacientes.find_one(
                {'Paciente_ID': str(paciente_id)},
                {'_id': 0}
            )
        except:
            return None

    def get_pacientes_con_alergias(self):
        return list(self.db.pacientes.find(
            {'Alergias': {'$nin': ['[]', '', None]}}, 
            {'_id': 0, 'Paciente_ID': 1, 'Nombre': 1, 'Apellido': 1, 'Alergias': 1}
        )
    )

    def buscar_pacientes_por_nombre(self, texto):
        return list(self.db.pacientes.find({
            '$or': [
                {'Nombre': {'$regex': texto, '$options': 'i'}},
                {'Apellido': {'$regex': texto, '$options': 'i'}}
            ]
        }, {
            '_id': 0, 'Paciente_ID': 1, 'Nombre': 1, 'Apellido': 1, 'Telefono': 1
        }))

    #  MEDICAMENTO

    def get_all_medicamentos(self):
        return list(self.db.medicamentos.find({}, {'_id': 0}))

    def get_medicamentos_por_principio(self, principio):
        return list(self.db.medicamentos.find({
            'Principio_Activo': {'$regex': principio, '$options': 'i'}
        }, {'_id': 0, 'Medicamento_ID': 1, 'Nombre': 1, 'Dosis': 1, 'Stock': 1}))

    def get_medicamentos_bajo_stock(self, limite=20):
        return list(self.db.medicamentos.find({
            'Stock': {'$lte': limite}
        }, {'_id': 0}).sort('Stock', 1))

    #  EXPEDIENTES

    def get_expediente_by_paciente_id(self, paciente_id):
        try:
            return self.db.expedientes.find_one(
                {'Paciente_ID': int(paciente_id)},
                {'_id': 0}
            )
        except:
            return None

    def get_diagnosticos_frecuentes(self, limite=5):
        pipeline = [
            {'$unwind': '$Diagnosticos'},
            {'$group': {
                '_id': '$Diagnosticos',
                'cantidad': {'$sum': 1}
            }},
            {'$sort': {'cantidad': -1}},
            {'$limit': limite}
        ]
        return list(self.db.expedientes.aggregate(pipeline))

    def get_pacientes_con_tratamiento(self, medicamento_nombre):
        pipeline = [
            {'$match': {'Tratamientos.medicamento': {'$regex': medicamento_nombre, '$options': 'i'}}},
            {'$lookup': {
                'from': 'pacientes',
                'localField': 'Paciente_ID',
                'foreignField': 'Paciente_ID',
                'as': 'paciente_info'
            }},
            {'$unwind': '$paciente_info'},
            {'$project': {
                '_id': 0,
                'Paciente_ID': 1,
                'Nombre': '$paciente_info.Nombre',
                'Apellido': '$paciente_info.Apellido',
                'Tratamiento': {
                    '$filter': {
                        'input': '$Tratamientos',
                        'as': 't',
                        'cond': {'$regexMatch': {
                            'input': '$$t.medicamento',
                            'regex': medicamento_nombre,
                            'options': 'i'
                        }}
                    }
                }
            }}
        ]
        return list(self.db.expedientes.aggregate(pipeline))