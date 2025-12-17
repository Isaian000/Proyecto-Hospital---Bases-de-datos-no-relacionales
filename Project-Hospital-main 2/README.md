# Plataforma de Integración de Datos de Salud

## Información del Equipo
- **Isaian Ayala Garcia** - 751789
- **Emilio Castillon Martin** - 739520
- **Jesus Vargas Pacheco** - 750962

## Descripción del Proyecto

La Plataforma de Integración de Datos de Salud es un sistema multi-base de datos diseñado para gestionar información médica de manera eficiente utilizando tres tecnologías diferentes de bases de datos: MongoDB, Cassandra y Dgraph. Cada una aprovecha sus fortalezas específicas para diferentes tipos de consultas y operaciones.

### Objetivo

Crear un sistema integral que permita:
- **Gestionar** información de pacientes, doctores, hospitales, citas y tratamientos
- **Consultar** datos médicos de forma eficiente según diferentes patrones de acceso
- **Analizar** relaciones complejas entre entidades médicas
- **Optimizar** el rendimiento mediante el uso apropiado de cada tecnología de base de datos

### ¿Para qué sirve?

El sistema permite a instituciones de salud:
1. Mantener registros médicos completos y actualizados
2. Programar y gestionar citas médicas
3. Rastrear diagnósticos, tratamientos y prescripciones
4. Realizar análisis estadísticos y reportes de salud
5. Identificar relaciones entre pacientes, condiciones y tratamientos
6. Garantizar la trazabilidad de historiales médicos


## Instalación y Configuración

### Prerequisitos
- Docker
- Python 3.8+
- pip (gestor de paquetes de Python)

# Si pip no esta en tu sistema:
sudo apt update
sudo apt install python3-pip

# Instala y Activa el ambiente virtual (venv) (Linux/MacOS)
python -m pip install virtualenv
python -m venv venv
source venv/bin/activate

# Instala y Activa el ambiente virtual (venv) (Windows)
python -m pip install virtualenv
python -m venv venv
venv\Scripts\Activate.ps1

# Instala dependencias del proyecto
pip install -r requirements.txt

# En una terminal nueva, inicia los siguintes contenedores:
docker run --name cassandradb -d -p 9042:9042 cassandra
docker run --name mongodb -d -p 27017:27017 mongo
docker run --name dgraph -d -p 8080:8080 -p 9080:9080  dgraph/standalone

# Carga los datos en el ambiente virtual activado (venv)
python3 populate.py

# Inicia la aplicacion
python3 main.py


## Acceso al Repositorio

- **Repositorio**: [GitHub - Plataforma-de-Integraci-n-de-Datos-de-Salud](https://github.com/Isaian000/Plataforma-de-Integraci-n-de-Datos-de-Salud)
- **Acceso de lectura para**: HomerMadriz

## Contacto

Para más información sobre este proyecto, contactar a cualquiera de los miembros del equipo.