# DGraph/graph.py

SCHEMA = """
# ----- Tipos -----
type Patient {
  patient_id
  name
  has_primary_doctor
  care_team
  treated_with
  emergency_contact
}

type Doctor {
  doctor_id
  name
  specialty
  rating
  works_at
}

type Medicine {
  medicine_id
  name
  dose_mg
  interacts_with
}

type Treatment {
  treatment_id
  description
  route
  frequency
  uses_medicine
  for_diagnosis
}

type Diagnosis {
  diagnosis_id
  name
}

type Clinic {
  clinic_id
  name
  city
}

type EmergencyContact {
  contact_id
  name
  phone
  relationship
}

# ----- Predicados / Atributos -----

patient_id: string @index(exact) .
doctor_id: string @index(exact) .
medicine_id: string @index(exact) .
treatment_id: string @index(exact) .
diagnosis_id: string @index(exact) .
clinic_id: string @index(exact) .
contact_id: string @index(exact) .

name: string @index(term, fulltext) .
specialty: string @index(term) .
rating: float @index(float) .
dose_mg: float .
description: string @index(fulltext) .
route: string .
frequency: string .
city: string @index(term) .
phone: string .
relationship: string .

# Relaciones (uid)
has_primary_doctor: uid @reverse .
care_team: uid @reverse .
treated_with: uid @reverse .
uses_medicine: uid @reverse .
for_diagnosis: uid @reverse .
works_at: uid @reverse .
emergency_contact: uid @reverse .
interacts_with: uid @reverse @count .
"""

from connect import create_client
import pydgraph

def set_schema():
    client = create_client()
    op = pydgraph.Operation(schema=SCHEMA)
    client.alter(op)
    print("[OK] Schema aplicado.")
