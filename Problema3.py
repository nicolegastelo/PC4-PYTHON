import os

def contar_lineas_codigo(ruta_archivo):
    try:
        
        if not ruta_archivo.endswith(".py"):
            print("El archivo no tiene extensión .py")
            return
        
        
        if not os.path.exists(ruta_archivo):
            print(f"El archivo {ruta_archivo} no existe.")
            return
        
       
        with open(ruta_archivo, "r") as file:
            lineas = file.readlines()
        
        
        lineas_codigo = 0
        
        for linea in lineas:
           
            linea = linea.strip()
            if linea and not linea.startswith("#"):  
                lineas_codigo += 1
        
        print(f"El archivo {ruta_archivo} tiene {lineas_codigo} líneas de código.")
    
    except FileNotFoundError:
        print(f"El archivo {ruta_archivo} no se encuentra.")
    except Exception as e:
        print(f"Ha ocurrido un error: {e}")


def obtener_ruta_archivo():
    ruta = input("Introduce la ruta del archivo .py: ")
    contar_lineas_codigo(ruta)


obtener_ruta_archivo()