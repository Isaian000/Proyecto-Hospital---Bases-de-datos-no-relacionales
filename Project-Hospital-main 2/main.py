#-------------------------------------------------------------------------------------------------------
# MONGODB MAIN
from MongoDB.model import HospitalModel
import os
from datetime import date
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calcular_edad(fecha_nac):
    """Calcula la edad a partir de una fecha en formato YYYY-MM-DD"""
    try:
        nacimiento = date.fromisoformat(fecha_nac)
        hoy = date.today()
        edad = hoy.year - nacimiento.year - ((hoy.month, hoy.day) < (nacimiento.month, nacimiento.day))
        return edad
    except:
        return "N/A"

def print_menu_Mongodb():
    """Imprime el menú principal"""
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=" * 60)
    print("       SISTEMA HOSPITALARIO - CONSULTAS MONGODB")
    print("=" * 60)
    print("1. Listar todos los pacientes")
    print("2. Buscar paciente por ID")
    print("3. Pacientes con alergias registradas")
    print("4. Buscar paciente por nombre o apellido")
    print("5. Listar todos los medicamentos")
    print("6. Medicamentos con stock bajo")
    print("7. Buscar medicamento por principio activo")
    print("8. Ver expediente clínico por ID de paciente")
    print("9. Diagnósticos más frecuentes")
    print("10. Pacientes que toman un medicamento específico")
    print("0. Salir")
    print("=" * 60)

def main_mongoDB():
    model = HospitalModel()
    if not model.connect():
        print("ERROR: No se pudo conectar a la base de datos")
        input("Presiona Enter para salir...")
        return

    try:
        while True:
            print_menu_Mongodb()
            option = input("Selecciona una opción: ").strip()

            if option == '1':
                print("\nLISTADO DE PACIENTES")
                print("-" * 80)
                pacientes = model.get_all_pacientes()
                for p in pacientes:
                    edad = calcular_edad(p['Fecha_de_Nacimiento'])
                    print(f"ID: {p['Paciente_ID']:<6} | {p['Nombre']} {p['Apellido']:<25} | Edad: {edad:<3} | Tel: {p['Telefono']}")
                print("-" * 80)

            elif option == '2':
                pid = input("Ingresa el ID del paciente (ej. P001): ").strip()
                paciente = model.get_paciente_by_id(pid)
                if not paciente:
                    print("Paciente no encontrado.")
                else:
                    print("\nDETALLE DEL PACIENTE")
                    print("-" * 60)
                    edad = calcular_edad(paciente['Fecha_de_Nacimiento'])
                    print(f"ID            : {paciente['Paciente_ID']}")
                    print(f"Nombre        : {paciente['Nombre']} {paciente['Apellido']}")
                    print(f"Edad          : {edad} años")
                    print(f"Teléfono      : {paciente['Telefono']}")
                    print(f"Ciudad        : {paciente['Domicilio_Ciudad']}")
                    print(f"Género        : {paciente['Genero']}")
                    print(f"Ocupación     : {paciente['Ocupacion']}")
                    print(f"Alergias      : {(paciente['Alergias']) if paciente['Alergias'] else 'Ninguna'}")
                    print(f"Emergencia    : {paciente['Datos_emergencia_Nombre']} ({paciente['Datos_emergencia_Parentesco']}) - {paciente['Datos_emergencia_Celular']}")
                    print("-" * 60)

            elif option == '3':
                print("\nPACIENTES CON ALERGIAS REGISTRADAS")
                print("-" * 80)
                pacientes = model.get_pacientes_con_alergias()
                for p in pacientes:
                    print(f"ID: {p['Paciente_ID']:<6} | {p['Nombre']} {p['Apellido']:<25} | Alergias: {(p['Alergias'])}")
                print("-" * 80)

            elif option == '4':
                texto = input("Buscar por nombre o apellido: ").strip()
                print(f"\nRESULTADOS PARA: {texto.upper()}")
                print("-" * 60)
                pacientes = model.buscar_pacientes_por_nombre(texto)
                for p in pacientes:
                    print(f"ID: {p['Paciente_ID']:<6} | {p['Nombre']} {p['Apellido']:<25} | Tel: {p['Telefono']}")
                print("-" * 60)

            elif option == '5':
                print("\nINVENTARIO DE MEDICAMENTOS")
                print("-" * 90)
                meds = model.get_all_medicamentos()
                for m in meds:
                    print(f"ID: {m['Medicamento_ID']:<3} | {m['Nombre']:<20} | {m['Principio_Activo']:<25} | Dosis: {m['Dosis']:<8} | Stock: {m['Stock']:<4} | Vence: {m['Fecha_Vencimiento']}")
                print("-" * 90)

            elif option == '6':
                print("\nMEDICAMENTOS CON STOCK BAJO (<= 50 unidades)")
                print("-" * 70)
                meds = model.get_medicamentos_bajo_stock(50)
                for m in meds:
                    print(f"ID: {m['Medicamento_ID']:<3} | {m['Nombre']:<20} | Stock: {m['Stock']:<4} | Fabricante: {m['Fabricante']}")
                print("-" * 70)

            elif option == '7':
                principio = input("Principio activo a buscar: ").strip()
                print(f"\nRESULTADOS PARA: {principio.upper()}")
                print("-" * 70)
                meds = model.get_medicamentos_por_principio(principio)
                for m in meds:
                    print(f"ID: {m['Medicamento_ID']:<3} | {m['Nombre']:<20} | Dosis: {m['Dosis']:<8} | Stock: {m['Stock']}")
                print("-" * 70)

            elif option == '8':
                pid = input("Ingresa el ID del paciente (ej. P001): ").strip()
                expediente = model.get_expediente_by_paciente_id(pid)
                if not expediente:
                    print("No se encontró expediente para este paciente.")
                else:
                    print(f"\nEXPEDIENTE CLÍNICO - Paciente ID: {pid}")
                    print("-" * 80)
                    print(f"Fecha creación: {expediente['Fecha_Creacion']}")
                    print("\nCITAS:")
                    for c in expediente['Citas']:
                        print(f"  {c['fecha']} - {c['motivo']} ({c['medico']})")
                    print("\nDIAGNÓSTICOS:")
                    for d in expediente['Diagnosticos']:
                        print(f"  - {d}")
                    print("\nTRATAMIENTOS:")
                    for t in expediente['Tratamientos']:
                        print(f"  - {t['medicamento']} | {t['dosis']} | {t['frecuencia']}")
                    print("-" * 80)

            elif option == '9':
                print("\nDIAGNÓSTICOS MÁS FRECUENTES")
                print("-" * 50)
                diagnosticos = model.get_diagnosticos_frecuentes(10)
                for item in diagnosticos:
                    print(f"{item['_id']:<35} | {item['cantidad']} casos")
                print("-" * 50)

            elif option == '10':
                med = input("Nombre del medicamento a buscar: ").strip()
                print(f"\nPACIENTES QUE TOMAN: {med.upper()}")
                print("-" * 70)
                pacientes = model.get_pacientes_con_tratamiento(med)
                for p in pacientes:
                    print(f"ID: {p['Paciente_ID']:<6} | {p['Nombre']} {p['Apellido']:<25} | Tratamiento: {p['Tratamiento']}")
                print("-" * 70)

            elif option == '0':
                print("Saliendo del sistema. Hasta luego.")
                break

            else:
                print("Opción no válida.")

            input("\nPresiona Enter para continuar...")

    except KeyboardInterrupt:
        print("\n\nOperación cancelada por el usuario.")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
    finally:
        model.close()

#-------------------------------------------------------------------------------------------------------
#CASSANDRA MAIN

import logging
import os
import sys

from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from Cassandra import model_cass


# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'cassandra_project')
REPLICATION_FACTOR = int(os.getenv('CASSANDRA_REPLICATION_FACTOR', '1'))

def print_menu_Cassandra():
    mm_options = {
        0: "Populate sample data",
        1: "Show heart rate, steps and spo2 (Q1)",
        2: "Show heart rate and glucose by interval (Q2)",
        3: "Show glucose by patient (Q3)",
        4: "Show last read by N patients (Q4)",
        5: "Show last read by one patient (Q5)",
        6: "Show out-of-range readings for patient (Q6)",
        7: "Show heart_rate stats by patient (Q7)",
        8: "Show last N reads by patient (Q8)",
        9: "Show readings by patient (Q9)",
        10: "Show weareable info by patient (Q10)",
        11: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def main_Cassandra():

    cluster = Cluster(
        CLUSTER_IPS.split(','),
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
        protocol_version=5
    )
    session = cluster.connect()

    model_cass.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session = cluster.connect(KEYSPACE)

    # create tables if needed
    model_cass.create_schema(session)

    try:
        while True:
            print("\n" + "="*50)
            print_menu_Cassandra()
            try:
                option = int(input('\nEnter your choice: '))
            except ValueError:
                print("Please enter a valid number.")
                continue

            if option == 0:
                print("Populating sample data...")
                model_cass.bulk_insert(session)
                print("Sample data populated successfully!")

            elif option == 1:
                patient_id = input('Enter patient ID: ')
                print(f"\nQ1: Show heart rate, steps and spo2 for patient {patient_id}")
                rows = model_cass.get_data_by_patient(session, patient_id)
                for r in rows:
                    print(r)

            elif option == 2:
                patient_id = input('Enter patient ID: ')
                start = input('Enter start datetime (YYYY-MM-DD HH:MM:SS): ')
                end = input('Enter end datetime (YYYY-MM-DD HH:MM:SS): ')
                try:
                    start_dt = start
                    end_dt = end
                    rows = model_cass.get_readings_by_patient_interval(session, patient_id, start_dt, end_dt)
                    for r in rows:
                        print(r)
                except Exception as e:
                    print("Error parsing dates or querying:", e)

            elif option == 3:
                patient_id = input('Enter patient ID: ')
                print(f"\nQ3: Show glucose by patient: {patient_id}")
                rows = model_cass.get_glucose_by_patient(session, patient_id)
                for r in rows:
                    print(r)

            elif option == 4:
                ids = input('Enter comma-separated patient IDs: ')
                patient_list = [i.strip() for i in ids.split(',') if i.strip()]
                rows = model_cass.get_last_read_by_n_patients(session, patient_list)
                for r in rows:
                    print(r)

            elif option == 5:
                patient_id = input('Enter patient ID: ')
                row = model_cass.get_last_read_by_patient(session, patient_id)
                print(row)

            elif option == 6:
                patient_id = input('Enter patient ID: ').strip()
                if not patient_id:
                    print("Patient ID vacío.")
                else:
                    out_rows = model_cass.get_out_of_range_by_patient(session, patient_id)
                    if not out_rows:
                        print(f"No out-of-range readings for patient {patient_id}.")
                    else:
                        print(f"Out-of-range readings for patient {patient_id}:")
                        for r in out_rows:
                            print(r)

            elif option == 7:
                patient_id = input('Enter patient ID for heart rate stats: ').strip()
                if not patient_id:
                    print("Patient ID vacío.")
                else:
                    stats = model_cass.get_heart_rate_stats(session, patient_id)
                    print(f"Heart rate stats for {patient_id}: count={stats['count']}, avg={stats['avg']}, min={stats['min']}, max={stats['max']}")

            elif option == 8:
                patient_id = input('Enter patient ID: ')
                n = input('Enter N (number of reads): ')
                try:
                    n_int = int(n)
                except ValueError:
                    n_int = 10
                rows = model_cass.get_last_n_reads_by_patient(session, patient_id, n_int)
                for r in rows:
                    print(r)
            
            elif option == 9:
                patient_id = input('Enter patient ID to count reads: ').strip()
                if not patient_id:
                    print("Patient ID vacío.")
                else:
                    count = model_cass.get_readings_count_by_patient(session, patient_id)
                    print(f"Patient {patient_id} has {count} readings.")

            elif option == 10:
                weareable_id = input('Enter patient ID to look up wearable info: ')
                rows = model_cass.get_weareable_info_by_patient_id(session, weareable_id)
                for r in rows:
                    print(r)

            elif option == 11:
                print("Exiting application...")
                break

            else:
                print("Invalid option. Please try again.")
    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        try:
            cluster.shutdown()
        except Exception:
            pass

#-------------------------------------------------------------------------------------------------------
#DGRAPH MAIN

from connect import create_client
from DGraph import model_graph

client = create_client()

# Consultas de primary doctor, care team, medicines by patient, patients by doctor
import json

def main_dgraph():
    while True:
        print("\n=== Hospital – Módulo Dgraph ===")
        print("1. Query: Registro y vínculo de atención primaria")
        print("2. Query: Mapa de equipo de cuidado por paciente")
        print("3. Query: Historial de medicamentos")
        print("4. Query: Plan terapéutico dirigido por diagnóstico")
        print("5. Query: Detección de interacciones medicamento-medicamento")
        print("6. Query: Camino de atención")
        print("7. Query: Búsqueda pacientes por médico")
        print("8. Query: Recomendación de especialista")
        print("9. Query: Búsqueda de contactos de emergencia")
        print("10. Query: Clínicas en las que opera un doctor")
        print("11. Salir")
        op = input("> ")

        # Procesar la opción seleccionada con un if-elif-else
    
        if op == "1":
            model_graph.query_registro_atencion_primaria()
        elif op == "2":
            model_graph.query_equipo_cuidado()
        elif op == "3":
            model_graph.query_historial_medicamentos()
        elif op == "4":
            model_graph.query_plan_terapeutico()
        elif op == "5":
            model_graph.query_interacciones_medicamento()
        elif op == "6":
            model_graph.query_camino_atencion()
        elif op == "7":
            model_graph.query_pacientes_por_medico()
        elif op == "8":
            model_graph.query_recomendacion_especialista()
        elif op == "9":
            model_graph.query_contactos_emergencia()
        elif op == "10":
            model_graph.query_clinicas_doctor()
        elif op == "11":
            break

            # Si vas a trabajar con más modulos como Cassandra o MongoDB, puedes modificar la opcion de salir para regresar al menu principal de selección de modulos con algo como esto:
            # elif op == "0":
            #     return  # Regresa al menú principal de selección de módulos



        else:
            print("Opción no válida.")
            # Esperar antes de mostrar el menú nuevamente
            input("Presiona Enter para continuar...")

#-------------------------------------------------------------------------------------------------------
#MENU PRINCIPAL DE LA APP - ESTO NO SE TOCA
def print_MAIN_menu():
    """Imprime el menú principal"""

    print("PROYECTO SISTEMA HOSPITAL - BASES DE DATOS")
    print("=" * 60)
    print("1.   Cassandra")
    print("2.   MongoDB")
    print("3.   Dgraph")
    print("0. Salir")

def main():
        """Función principal de la aplicación"""

        while True:
            print_MAIN_menu()
            
            try:
                option = input("Selecciona una opción: ").strip()
                
                if option == '1':
                    #OPCION CASSANDRA
                    main_Cassandra()
                
                elif option == '2':
                    #OPCION DE MONGODB
                    main_mongoDB()
                
                elif option == '3':
                    #OPCION DE DGRAPH
                    main_dgraph()
                
                elif option == '0':
                    print("👋 ¡Gracias por usar el sistema!")
                    break
                
                else:
                    print("❌ Opción inválida")
                    input("Presiona Enter para continuar...")
                    
            except ValueError:
                print("❌ Error: Ingresa un valor numérico válido")
                input("Presiona Enter para continuar...")
            except KeyboardInterrupt:
                print("\n\n👋 Saliendo...")
                break
            except Exception as e:
                logger.error(f"❌ Error: {e}")
                input("Presiona Enter para continuar...")


if __name__ == "__main__":
    main()


