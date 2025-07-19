import pandas as pd
from pymongo import MongoClient
import sqlite3
from datetime import datetime


client = MongoClient("mongodb://localhost:27017/")
db = client["empresa"]
coleccion_ventas = db["ventas"]
coleccion_tipo_cambio = db["tipo_cambio"]


ventas_df = pd.read_csv("ventas.csv")


print(ventas_df.head())  


conn = sqlite3.connect('tipo_cambio.db')
cursor = conn.cursor()


def obtener_tipo_cambio(fecha):
    cursor.execute("SELECT cambio FROM tipo_cambio WHERE fecha = ?", (fecha,))
    resultado = cursor.fetchone()
    if resultado:
        return resultado[0]
    else:
        return 1  


def solarizar_precio(fecha, precio_dolares):
    tipo_cambio = obtener_tipo_cambio(fecha)
    precio_solarizado = precio_dolares * tipo_cambio
    return precio_solarizado


ventas_solarizadas = []


for index, row in ventas_df.iterrows():
    fecha = row["fecha"] 
    producto = row["producto"]
    precio_dolares = row["precio_dolares"]

   
    print(f"Procesando venta: Producto={producto}, Fecha={fecha}, Precio (USD)={precio_dolares}")
    
   
    precio_solarizado = solarizar_precio(fecha, precio_dolares)

   
    venta_solarizada = {
        "producto": producto,
        "fecha": fecha,
        "precio_solarizado": precio_solarizado
    }
    ventas_solarizadas.append(venta_solarizada)


for item in ventas_solarizadas:
    print(item)  


coleccion_ventas.insert_many(ventas_solarizadas)


conn.close()

print("Ventas solarizadas almacenadas en MongoDB.")
