#!/usr/bin/env python3
from datetime import datetime
import logging
import random
import uuid

from cassandra.query import BatchStatement

# set logger

log = logging.getLogger()


# create keyspace

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""


# create tables

CREATE_WEAREABLE_INFO_TABLE = """
    CREATE TABLE IF NOT EXISTS weareable_info (
        device_id TEXT PRIMARY KEY,
        device_name TEXT,
        patient_id TEXT
    )
"""

CREATE_DATA_BY_PATIENT_TABLE = """
        CREATE TABLE IF NOT EXISTS data_by_patient (
            patient_id TEXT,
            name TEXT,
            steps INT,
            heart_rate INT,
            spo2 INT,
            glucose INT,
            timestmp TIMEUUID,
            PRIMARY KEY (patient_id, timestmp)
        ) WITH CLUSTERING ORDER BY (timestmp DESC)
"""

CREATE_ALERTS_TABLE = """
    CREATE TABLE IF NOT EXISTS alerts_by_patient (
        alert_id TEXT,
        patient_id TEXT,
        alert_type TEXT,
        value INT,
        threshold INT,
        severity TEXT,
        description TEXT,
        timestmp TIMEUUID,
        PRIMARY KEY (patient_id, timestmp)
    ) WITH CLUSTERING ORDER BY (timestmp DESC)
"""


INSERT_ALERT = """
    INSERT INTO alerts_by_patient (
        alert_id, patient_id, alert_type, value, threshold, severity, description, timestmp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


# query statments

#Q1

SELECT_DATA_BY_PATIENT = """
    SELECT patient_id, name, heart_rate, spo2, steps, timestmp
    FROM data_by_patient
    WHERE patient_id = ?
"""

#Q2

SELECT_READINGS_BY_PATIENT_INTERVAL = """
    SELECT patient_id, name, heart_rate, glucose, timestmp
    FROM data_by_patient
    WHERE patient_id = ?
      AND timestmp >= minTimeuuid(?)
      AND timestmp <= maxTimeuuid(?)
"""

#Q3

SELECT_GLUCOSE_BY_PATIENT = """
    SELECT patient_id, name, glucose, timestmp
    FROM data_by_patient
    WHERE patient_id = ?
"""

#Q4

SELECT_LAST_READ_BY_N_PATIENTS = """
    SELECT patient_id, heart_rate, spo2, glucose, timestmp
    FROM data_by_patient
    WHERE patient_id IN ?

"""


#Q5

SELECT_LAST_READ_BY_PATIENT = """
    SELECT *
    FROM data_by_patient
    WHERE patient_id = ?
    LIMIT 1
"""


#Q6

SELECT_HEARTRATE_OUT = """
    SELECT patient_id, name, heart_rate, glucose, spo2, timestmp
    FROM data_by_patient
    WHERE patient_id = ?
        AND heart_rate > ?
    ALLOW FILTERING
"""

SELECT_GLUCOSE_OUT = """
    SELECT patient_id, name, heart_rate, glucose, spo2, timestmp
    FROM data_by_patient
    WHERE patient_id = ?
        AND glucose > ?
    ALLOW FILTERING
"""

SELECT_SPO2_OUT = """
    SELECT patient_id, name, heart_rate, glucose, spo2, timestmp
    FROM data_by_patient
    WHERE patient_id = ?
        AND spo2 < ?
    ALLOW FILTERING
"""

#Q7

SELECT_HEART_RATE_BY_PATIENT = """
    SELECT patient_id, name, heart_rate, timestmp
    FROM data_by_patient
    WHERE patient_id = ?

"""

#Q8

SELECT_LAST_N_READS_BY_PATIENT = """
    SELECT *
    FROM data_by_patient
    WHERE patient_id = ?
    LIMIT ?
"""


#Q9

SELECT_COUNT_READINGS_BY_PATIENT = """
    SELECT COUNT(*) AS cnt
    FROM data_by_patient
    WHERE patient_id = ?
"""

#Q10

SELECT_WEAREABLE_INFO_BY_PATIENT_ID = """
    SELECT device_id, device_name
    FROM weareable_info
    WHERE patient_id = ?
    ALLOW FILTERING
"""

"""
En algunos queries estoy usando ALLOW FLITERING para poder acceder a las columnas que no son parte de la clave primaria.
Esto puede afectar el rendimiento en tablas grandes.
"""

# sample data


PATIENTS_INFO = [
    # patient_id, name, steps, heart_rate, spo2, glucose
    ('P001', 'Carlos Hernandez', 2023, 98, 100, 100),
    ('P002', 'Maria Lopez', 5026, 87, 96, 95),
    ('P003', 'Jose Martinez', 7024, 76, 98, 87),
    ('P004', 'Ana Garcia', 3500, 90, 97, 110),
    ('P005', 'Luis Rodriguez', 12000, 65, 99, 80),
    ('P006', 'Carlos Hernandez', 8000, 72, 95, 105),
    ('P007', 'Miguel Sanchez', 4500, 145, 98, 92),
    ('P008', 'Laura Ramirez', 2000, 68, 92, 66),
    ('P009', 'Jorge Flores', 4000, 160, 100, 115),
    ('P010', 'Elena Morales' , 9500, 75, 97, 89),
    ('P011', 'Ricardo Vargas', 11000, 80, 94, 78),
    ('P012', 'Patricia Cruz', 3000, 130, 99, 120),
    ('P013', 'Diego Navarro', 6000, 70, 96, 85),
    ('P014', 'Lucia Herrera', 7500, 88, 93, 98),
    ('P015', 'Fernando Castillo', 500, 155, 91, 130),
]

WEAREABLE_INFO = [
    # device_id, device_name, patient_id
    ('D001', 'Fitbit Charge 5', 'P001'),
    ('D002', 'Apple Watch Series 7', 'P002'),
    ('D003', 'Garmin Venu 2', 'P003'),
    ('D004', 'Samsung Galaxy Watch 4', 'P004'),
    ('D005', 'Xiaomi Mi Band 6', 'P005'),
    ('D006', 'Huawei Watch GT 3', 'P006'),
    ('D007', 'Amazfit GTR 3', 'P007'),
    ('D008', 'Withings Steel HR', 'P008'),
    ('D009', 'Polar Vantage M2', 'P009'),
    ('D010', 'Suunto 7', 'P010'),
    ('D011', 'Fossil Gen 5E', 'P011'),
    ('D012', 'TicWatch Pro 3', 'P012'),
    ('D013', 'Honor Band 6', 'P013'),
    ('D014', 'Realme Watch S Pro', 'P014'),
    ('D015', 'Oura Ring Generation 3', 'P015'),
]




# funciones

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creating tables...")
    session.execute(CREATE_WEAREABLE_INFO_TABLE)
    session.execute(CREATE_DATA_BY_PATIENT_TABLE)
    session.execute(CREATE_ALERTS_TABLE)
    log.info("Tables created successfully.")

def execute_batch(session, stmt, data):
    batch_size = 25
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)

def bulk_insert(session):
    """
    Inserta los datos de PATIENTS_INFO y WEAREABLE_INFO en las tablas
    Se genera timestmp (TIMEUUID) para cada lectura 
    """
    patients_stmt = session.prepare(
        "INSERT INTO data_by_patient (patient_id, name, steps, heart_rate, spo2, glucose, timestmp) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    weareable_stmt = session.prepare(
        "INSERT INTO weareable_info (device_id, device_name, patient_id) VALUES (?, ?, ?)"
    )

    # preparar lista con timestmp para cada paciente
    patients_with_ts = []
    now = None
    for p in PATIENTS_INFO:
        # p: (patient_id, name, steps, heart_rate, spo2, glucose)
        # generamos un TIMEUUID por cada inserción
        ts = uuid.uuid1()
        patients_with_ts.append((p[0], p[1], p[2], p[3], p[4], p[5], ts))

    # weareable data ya está en formato (device_id, device_name, patient_id)
    execute_batch(session, patients_stmt, patients_with_ts)
    execute_batch(session, weareable_stmt, WEAREABLE_INFO)




# Funciones para ejecutar las consultas

def get_data_by_patient(session, patient_id, limit=None):
    stmt = session.prepare(SELECT_DATA_BY_PATIENT)
    rows = session.execute(stmt, (patient_id,))
    results = list(rows)
    return results if limit is None else results[:limit]


def get_readings_by_patient_interval(session, patient_id, start_dt, end_dt, limit=None):
    """
    Recupera lecturas (heart_rate, glucose, timestmp) entre start_dt y end_dt (datetime).
    Convierte strings a datetime si es necesario.
    """

    if isinstance(start_dt, str):
        start_dt = datetime.strptime(start_dt, '%Y-%m-%d %H:%M:%S')
    if isinstance(end_dt, str):
        end_dt = datetime.strptime(end_dt, '%Y-%m-%d %H:%M:%S')
    
    stmt = session.prepare(SELECT_READINGS_BY_PATIENT_INTERVAL)
    rows = session.execute(stmt, (patient_id, start_dt, end_dt))
    results = list(rows)
    return results if limit is None else results[:limit]


def get_glucose_by_patient(session, patient_id, limit=None):
    """
    devuelve lecturas de glucosa para un paciente
    """
    stmt = session.prepare(SELECT_GLUCOSE_BY_PATIENT)
    rows = session.execute(stmt, (patient_id,))
    results = list(rows)
    return results if limit is None else results[:limit]


def get_last_read_by_n_patients(session, patient_ids):
    """
    devuelve ltima lectura (LIMIT 1) para un conjunto de pacientes
    """
    stmt = session.prepare(SELECT_LAST_READ_BY_N_PATIENTS)
    rows = session.execute(stmt, (tuple(patient_ids),))
    return list(rows)


def get_last_read_by_patient(session, patient_id):
    """
    devuelve la ultima lectura para un paciente
    """
    stmt = session.prepare(SELECT_LAST_READ_BY_PATIENT)
    rows = session.execute(stmt, (patient_id,))
    results = list(rows)
    return results[0] if results else None


def get_last_n_reads_by_patient(session, patient_id, n):
    """
    devuelve las ultimas n lecturas para un paciente
    """
    stmt = session.prepare(SELECT_LAST_N_READS_BY_PATIENT)
    rows = session.execute(stmt, (patient_id, n))
    return list(rows)


def get_weareable_info_by_patient_id(session, patient_id):
    """
    devuelve la informacion de dispositivo wearable por patient_id
    """
    stmt = session.prepare(SELECT_WEAREABLE_INFO_BY_PATIENT_ID)
    rows = session.execute(stmt, (patient_id,))
    return list(rows)


def get_readings_count_by_patient(session, patient_id):

    """
    Cuenta el numero de lecturas  para un patient_id.
    Devuelve int (0 si no hay resultados o en caso de error)
    """
    try:
        stmt = session.prepare(SELECT_COUNT_READINGS_BY_PATIENT)
        row = session.execute(stmt, (patient_id,)).one()
        if row is None:
            return 0
    
        return int(getattr(row, 'cnt', 0))
    except Exception as e:
        log.exception(f"Error contando lecturas para patient_id={patient_id}: {e}")
        return 0


def get_out_of_range_by_patient(session, patient_id, hr_threshold=140, glucose_threshold=120, spo2_threshold=90):
    """
    Recupera lecturas fuera de rango para un paciente:
    heart_rate > hr_threshold OR glucose > glucose_threshold OR spo2 < spo2_threshold
    """
    try:
        stmt_hr = session.prepare(SELECT_HEARTRATE_OUT)
        stmt_gl = session.prepare(SELECT_GLUCOSE_OUT)
        stmt_sp = session.prepare(SELECT_SPO2_OUT)

        combined = {}
        
        for stmt, params in [
            (stmt_hr, (patient_id, hr_threshold)),
            (stmt_gl, (patient_id, glucose_threshold)),
            (stmt_sp, (patient_id, spo2_threshold)),
        ]:
            rows = session.execute(stmt, params)
            for r in rows:
                key = str(getattr(r, 'timestmp', None))
                combined[key] = r

        return list(combined.values())
    except Exception as e:
        log.exception(f"Error obteniendo lecturas fuera de rango para patient_id={patient_id}: {e}")
        return []


def get_heart_rate_stats(session, patient_id):
    #regresa numero de lecturas de "heart_rate", promedio, min y max
    try:
        stmt = session.prepare(SELECT_HEART_RATE_BY_PATIENT)
        rows = session.execute(stmt, (patient_id,))
        hrs = [getattr(r, 'heart_rate', None) for r in rows]
        hrs = [h for h in hrs if h is not None]
        if not hrs:
            return {'count': 0, 'avg': None, 'min': None, 'max': None}
        return {
            'count': len(hrs),
            'avg': sum(hrs) / len(hrs),
            'min': min(hrs),
            'max': max(hrs)
        }
    except Exception as e:
        log.exception(f"Error calculando estadisticas de ritmo cardiaco para el paciente:={patient_id}: {e}")
        return {'count': 0, 'avg': None, 'min': None, 'max': None}