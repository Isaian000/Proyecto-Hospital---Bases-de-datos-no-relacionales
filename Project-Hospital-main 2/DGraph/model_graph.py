# DGraph/main.py
from connect import create_client

client = create_client()

# Consultas de primary doctor, care team, medicines by patient, patients by doctor
import json

# 1. Registro y vínculo de atención primaria
def query_registro_atencion_primaria():
    """
    Registrar en el grafo el vínculo "atención primaria" entre un paciente y su médico responsable.
    Flujo: Paciente --hasPrimaryDoctor--> Doctor con facetas {desde, estado}.
    """
    patient_id = input("patient_id: ")
    q = f"""
    {{
      patient(func: eq(patient_id, "{patient_id}")) {{
        patient_id
        name
        has_primary_doctor {{
          doctor_id
          name
          specialty
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Vínculo de Atención Primaria ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 2. Mapa de equipo de cuidado por paciente
def query_equipo_cuidado():
    """
    Consultar a todos los profesionales activos que atienden a un paciente.
    Resultado: Gráfica con todos los médicos y descripción de su rol.
    """
    patient_id = input("patient_id: ")
    q = f"""
    {{
      patient(func: eq(patient_id, "{patient_id}")) {{
        patient_id
        name
        has_primary_doctor {{
          doctor_id
          name
          specialty
        }}
        care_team {{
          doctor_id
          name
          specialty
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Equipo de Cuidado del Paciente ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 3. Historial de Medicamentos
def query_historial_medicamentos():
    """
    Consultar los medicamentos para el tratamiento de un paciente.
    Resultado: Medicamentos, fecha de caducidad, gramos (dosis).
    """
    patient_id = input("patient_id: ")
    q = f"""
    {{
      patient(func: eq(patient_id, "{patient_id}")) {{
        patient_id
        name
        treated_with {{
          treatment_id
          description
          route
          frequency
          uses_medicine {{
            medicine_id
            name
            dose_mg
          }}
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Historial de Medicamentos ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 4. Plan terapéutico dirigido por diagnóstico
def query_plan_terapeutico():
    """
    Dado un diagnóstico, recuperar el plan activo de tratamientos.
    Resultado: Relación con medicamento, vía de administración, frecuencia y dosis.
    """
    diagnosis_id = input("diagnosis_id: ")
    q = f"""
    {{
      diagnosis(func: eq(diagnosis_id, "{diagnosis_id}")) {{
        diagnosis_id
        name
        ~for_diagnosis {{
          treatment_id
          description
          route
          frequency
          uses_medicine {{
            medicine_id
            name
            dose_mg
          }}
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Plan Terapéutico por Diagnóstico ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 5. Detección de interacciones medicamento–medicamento
def query_interacciones_medicamento():
    """
    Consultar los posibles efectos secundarios de un medicamento que toma un paciente.
    Resultado: Efectos secundarios de un medicamento.
    """
    medicine_id = input("medicine_id: ")
    q = f"""
    {{
      medicine(func: eq(medicine_id, "{medicine_id}")) {{
        medicine_id
        name
        dose_mg
        interacts_with {{
          medicine_id
          name
          dose_mg
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Interacciones Medicamento-Medicamento ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 6. Camino de atención
def query_camino_atencion():
    """
    Construir la secuencia de eventos clínicos (visitas, estudios, tratamientos) como camino en el grafo.
    Resultado: Timeline ordenado, con nodos etiquetados por tipo de evento y referencias a IDs.
    """
    patient_id = input("patient_id: ")
    q = f"""
    {{
      patient(func: eq(patient_id, "{patient_id}")) {{
        patient_id
        name
        treated_with {{
          treatment_id
          description
          route
          frequency
          for_diagnosis {{
            diagnosis_id
            name
          }}
          uses_medicine {{
            medicine_id
            name
            dose_mg
          }}
        }}
        has_primary_doctor {{
          doctor_id
          name
          specialty
        }}
        care_team {{
          doctor_id
          name
          specialty
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Camino de Atención del Paciente ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 7. Búsqueda pacientes por médico (búsqueda por vecindad)
def query_pacientes_por_medico():
    """
    Encontrar pacientes conectados a un mismo médico.
    Resultado: Lista de pacientes de un médico, inicio de consulta.
    """
    doctor_id = input("doctor_id: ")
    q = f"""
    {{
      doctor(func: eq(doctor_id, "{doctor_id}")) {{
        doctor_id
        name
        specialty
        ~has_primary_doctor {{
          patient_id
          name
        }}
        ~care_team {{
          patient_id
          name
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Pacientes por Médico ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 8. Recomendación de especialista
def query_recomendacion_especialista():
    """
    Consultar los mejores médicos / especialistas de una clínica.
    Resultado: Top-N doctores con puntuación de recomendación y especialidad.
    """
    specialty = input("Especialidad (ej: Cardiology, Neurology): ")
    limit = input("Número de resultados (Top-N): ")
    q = f"""
    {{
      doctors(func: eq(specialty, "{specialty}"), orderdesc: rating, first: {limit}) {{
        doctor_id
        name
        specialty
        rating
        works_at {{
          clinic_id
          name
          city
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Recomendación de Especialistas ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 9. Búsqueda de contactos de emergencia del paciente
def query_contactos_emergencia():
    """
    Búsqueda y consulta de contactos de emergencia con sus datos y relación.
    Resultado: Nodos de relaciones de los contactos de emergencia, con nombre, teléfono y parentesco.
    """
    patient_id = input("patient_id: ")
    q = f"""
    {{
      patient(func: eq(patient_id, "{patient_id}")) {{
        patient_id
        name
        emergency_contact {{
          contact_id
          name
          phone
          relationship
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Contactos de Emergencia ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

# 10. Mostrar las clínicas en las que opera un doctor / médico
def query_clinicas_doctor():
    """
    Visualizar las clínicas en las que opera un doctor.
    Resultado: Mapa de clínicas en las que opera un doctor y su relación.
    """
    doctor_id = input("doctor_id: ")
    q = f"""
    {{
      doctor(func: eq(doctor_id, "{doctor_id}")) {{
        doctor_id
        name
        specialty
        works_at {{
          clinic_id
          name
          city
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(q)
    data = json.loads(res.json)
    print("\n=== Clínicas del Doctor ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))
