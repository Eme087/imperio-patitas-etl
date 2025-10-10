#!/bin/bash

# Este script automatiza los pasos iniciales para empezar a trabajar en el proyecto.

echo "--- Paso 1: Iniciando autenticación con Google Cloud ---"
# Abre el navegador para que inicies sesión con tu cuenta de Google.
gcloud auth login

echo ""
echo "--- Paso 2: Configurando el proyecto en Google Cloud ---"
# Establece 'imperio-patitas-cloud' como tu proyecto activo.
gcloud config set project imperio-patitas-cloud

echo ""
echo "--- Paso 3: Activando el entorno virtual de Python ---"
# Activa el venv para que tengas acceso a las librerías del proyecto.
source venv/bin/activate

echo ""
echo "✅ ¡Listo! Tu entorno de trabajo está preparado."
echo "Ahora estás autenticado, en el proyecto correcto y con el entorno virtual activado."