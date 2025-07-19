import requests
import sqlite3
from pymongo import MongoClient


def obtener_tipo_cambio_2023():
    url = "https://api.apis.net.pe/v1/tipo-cambio-sunat"
    params = {
        "fecha_inicio": "2023-01-01",
        "fecha_fin": "2023-12-31",
        "tipo_moneda": "USD"
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        try:
            datos = response.json()
            print("Datos obtenidos del API:", datos)  
            if isinstance(datos, list):
                return datos
            else:
                print("Los datos no están en el formato esperado. Asegúrate de que sean una lista de diccionarios.")
                return None
        except ValueError:
            print("Error al procesar los datos JSON.")
            return None
    else:
        print(f"Error al obtener los datos del API: {response.status_code}")
        return None


def almacenar_en_sqlite(datos):
    conn = sqlite3.connect('base.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sunat_info (
        fecha TEXT PRIMARY KEY,
        compra REAL,
        venta REAL
    )
    ''')

  
    for item in datos:
        if isinstance(item, dict) and 'fecha' in item and 'compra' in item and 'venta' in item:
            fecha = item['fecha']
            compra = item['compra']
            venta = item['venta']
            cursor.execute('''
            INSERT OR REPLACE INTO sunat_info (fecha, compra, venta)
            VALUES (?, ?, ?)
            ''', (fecha, compra, venta))

    conn.commit()
    conn.close()
    print("Datos almacenados en SQLite con éxito.")


def almacenar_en_mongo(datos):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sunat_db"]
    collection = db["sunat_info"]

   
    for item in datos:
        if isinstance(item, dict) and 'fecha' in item and 'compra' in item and 'venta' in item:
            collection.update_one(
                {"fecha": item['fecha']},
                {"$set": {"compra": item['compra'], "venta": item['venta']}},
                upsert=True
            )
    print("Datos almacenados en MongoDB con éxito.")


def mostrar_datos_sqlite():
    conn = sqlite3.connect('base.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sunat_info")
    rows = cursor.fetchall()
    print("\nDatos en SQLite:")
    for row in rows:
        print(f"Fecha: {row[0]}, Compra: {row[1]}, Venta: {row[2]}")
    conn.close()


def main():

    datos = obtener_tipo_cambio_2023()

    if datos:
        
        almacenar_en_sqlite(datos)
        almacenar_en_mongo(datos)
        
        
        mostrar_datos_sqlite()


if __name__ == "__main__":
    main()