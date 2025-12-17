#-------------------------------------------------------------------------------------------------------
#POPULATE FROM MONGODB

import csv
import logging
from connect import MongoDBConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_csv_data(file_path):
    "Carga datos desde archivo CSV"
    data = []
    with open(file_path, 'r', encoding='utf-8') as fd:
        csv_reader = csv.DictReader(fd)
        for row in csv_reader:
            row.pop('_id', None)
            data.append(row)
    logger.info(f"✅ Cargados {len(data)} registros desde {file_path}")
    return data

def populate_pacientes(db):
    #Llena colección de pacientes
    data = load_csv_data('data/Mongo/pacientes.csv')
    if data:
        result = db.pacientes.insert_many(data)
        logger.info(f"✅ Insertados {len(result.inserted_ids)} pacientes")
        return len(result.inserted_ids)
    return 0

def populate_medicamentos(db):
    #Llena colección de medicamentos
    data = load_csv_data('data/Mongo/medicamentos.csv')
    if data:
        result = db.medicamentos.insert_many(data)
        logger.info(f"✅ Insertados {len(result.inserted_ids)} medicamentos")
        return len(result.inserted_ids)
    return 0

def populate_expedientes(db):
    #Llena colección de expedientes
    data = load_csv_data('data/Mongo/expedientes.csv')
    if data:
        result = db.expedientes.insert_many(data)
        logger.info(f"✅ Insertadas {len(result.inserted_ids)} expedientes")
        return len(result.inserted_ids)
    return 0

def populate_mongodb():
    """Función principal para poblar la base de datos en MONGODB"""
    print("\nIniciando carga de datos a MONGODB...")
    
    # Conectar a MongoDB
    conn = MongoDBConnection()
    if not conn.connect():
        print(" No se pudo conectar a MongoDB")
        return
    try:
        db = conn.get_db()
        
        # Limpiar colecciones existentes
        print("Limpiando colecciones existentes...")
        db.pacientes.delete_many({})
        db.medicamentos.delete_many({})
        db.expedientes.delete_many({})
        
        # Poblar datos
        print("\n Cargando PACIENTES...")
        populate_pacientes(db)
 
        print(" Cargando MEDICAMENTOS...")
        populate_medicamentos(db)
 
        print(" Cargando EXPEDIENTES...\n")
        populate_expedientes(db)
     
        
    except Exception as e:
        logger.error(f"❌ Error durante la carga: {e}")
    finally:
        conn.close()

#-------------------------------------------------------------------------------------------------------
#POPULATE FROM DGRAPH

# DGraph/populate.py
import csv
from DGraph.graph import set_schema
from connect import create_client

DATA_DIR = "./data/Graph"

def load_patients(txn):
    with open(f"{DATA_DIR}/patients.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        muts = []
        for row in reader:
            muts.append({
                "uid": f"_:{row['patient_id']}",
                "dgraph.type": "Patient",
                "patient_id": row["patient_id"],
                "name": row["name"],
            })
        txn.mutate(set_obj=muts)

def load_doctors(txn):
    with open(f"{DATA_DIR}/doctors.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        muts = []
        for row in reader:
            muts.append({
                "uid": f"_:{row['doctor_id']}",
                "dgraph.type": "Doctor",
                "doctor_id": row["doctor_id"],
                "name": row["name"],
                "specialty": row["specialty"],
                "rating": float(row["rating"]),
            })
        txn.mutate(set_obj=muts)

def load_medicines(txn):
    with open(f"{DATA_DIR}/medicines.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        muts = []
        for row in reader:
            muts.append({
                "uid": f"_:{row['medicine_id']}",
                "dgraph.type": "Medicine",
                "medicine_id": row["medicine_id"],
                "name": row["name"],
                "dose_mg": float(row["dose_mg"]),
            })
        txn.mutate(set_obj=muts)

def load_treatments(txn):
    with open(f"{DATA_DIR}/treatments.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        muts = []
        for row in reader:
            treatment = {
                "uid": f"_:{row['treatment_id']}",
                "dgraph.type": "Treatment",
                "treatment_id": row["treatment_id"],
                "route": row["route"],
                "frequency": row["frequency"],
            }
            # Agregar description si existe en el CSV
            if "description" in row and row["description"]:
                treatment["description"] = row["description"]
            else:
                # Usar diagnosis como description si no hay description
                treatment["description"] = row.get("diagnosis", "Tratamiento")
            
            muts.append(treatment)
        txn.mutate(set_obj=muts)

def populate_dgraph():
    client = create_client()
    txn = client.txn()
    try:
        load_patients(txn)
        load_doctors(txn)
        load_medicines(txn)
        load_treatments(txn)
        txn.commit()
        print("[OK] Datos base cargados de DGRAPH.")
    except Exception as e:
        txn.discard()
        raise e
    
    # Ahora crear las relaciones en una nueva transacción
    txn = client.txn()
    try:
        create_relationships(txn)
        txn.commit()
        print("[OK] Relaciones creadas en DGRAPH.")
    except Exception as e:
        txn.discard()
        raise e

def create_relationships(txn):
    """Crea las relaciones entre pacientes, doctores, tratamientos y medicinas"""
    import csv
    import json
    
    # 1. Relacionar pacientes con sus médicos primarios
    with open(f"{DATA_DIR}/patients.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('primary_doctor_id'):
                # Buscar UID del paciente
                patient_query = f'{{ patient(func: eq(patient_id, "{row["patient_id"]}")) {{ uid }} }}'
                patient_res = txn.query(patient_query)
                patient_data = json.loads(patient_res.json)
                
                # Buscar UID del doctor
                doctor_query = f'{{ doctor(func: eq(doctor_id, "{row["primary_doctor_id"]}")) {{ uid }} }}'
                doctor_res = txn.query(doctor_query)
                doctor_data = json.loads(doctor_res.json)
                
                if patient_data.get('patient') and doctor_data.get('doctor'):
                    patient_uid = patient_data['patient'][0]['uid']
                    doctor_uid = doctor_data['doctor'][0]['uid']
                    
                    mutation = {
                        "uid": patient_uid,
                        "has_primary_doctor": {
                            "uid": doctor_uid
                        }
                    }
                    txn.mutate(set_obj=mutation)
    
    # 2. Relacionar tratamientos con pacientes, medicinas y crear care_team
    with open(f"{DATA_DIR}/treatments.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Buscar UIDs
            patient_query = f'{{ patient(func: eq(patient_id, "{row["patient_id"]}")) {{ uid }} }}'
            patient_res = txn.query(patient_query)
            patient_data = json.loads(patient_res.json)
            
            treatment_query = f'{{ treatment(func: eq(treatment_id, "{row["treatment_id"]}")) {{ uid }} }}'
            treatment_res = txn.query(treatment_query)
            treatment_data = json.loads(treatment_res.json)
            
            if patient_data.get('patient') and treatment_data.get('treatment'):
                patient_uid = patient_data['patient'][0]['uid']
                treatment_uid = treatment_data['treatment'][0]['uid']
                
                # Relacionar paciente con tratamiento
                patient_treatment = {
                    "uid": patient_uid,
                    "treated_with": {
                        "uid": treatment_uid
                    }
                }
                txn.mutate(set_obj=patient_treatment)
                
                # Relacionar tratamiento con medicina
                if row.get('medicine_id'):
                    medicine_query = f'{{ medicine(func: eq(medicine_id, "{row["medicine_id"]}")) {{ uid }} }}'
                    medicine_res = txn.query(medicine_query)
                    medicine_data = json.loads(medicine_res.json)
                    
                    if medicine_data.get('medicine'):
                        medicine_uid = medicine_data['medicine'][0]['uid']
                        treatment_medicine = {
                            "uid": treatment_uid,
                            "uses_medicine": {
                                "uid": medicine_uid
                            }
                        }
                        txn.mutate(set_obj=treatment_medicine)
                
                # Agregar doctor al care_team del paciente
                if row.get('doctor_id'):
                    doctor_query = f'{{ doctor(func: eq(doctor_id, "{row["doctor_id"]}")) {{ uid }} }}'
                    doctor_res = txn.query(doctor_query)
                    doctor_data = json.loads(doctor_res.json)
                    
                    if doctor_data.get('doctor'):
                        doctor_uid = doctor_data['doctor'][0]['uid']
                        care_team = {
                            "uid": patient_uid,
                            "care_team": {
                                "uid": doctor_uid
                            }
                        }
                        txn.mutate(set_obj=care_team)

#-------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    #funcion populate DGRAPHS
    set_schema()
    populate_dgraph()

    #funcion populate MONGODB
    populate_mongodb()

