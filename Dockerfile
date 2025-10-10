# Usar una imagen oficial de Python 3.9 (ligera y eficiente)
FROM python:3.9-slim

# Establecer un directorio de trabajo genérico
WORKDIR /code

# Copiar el archivo de requerimientos primero para aprovechar el cache de Docker
COPY requirements.txt requirements.txt

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación a una subcarpeta 'app'
COPY ./app ./app

# Exponer el puerto en el que correrá la aplicación.
# Cloud Run espera por defecto el puerto 8080.
EXPOSE 8080

# Comando para iniciar la aplicación usando uvicorn.
# Se ejecuta desde /code, por lo que Python reconoce 'app' como un paquete.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
