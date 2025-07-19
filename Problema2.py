import os

def guardar_tabla_multiplicar():
    try:
        
        n = int(input("Introduce un número entero entre 1 y 10 para la tabla de multiplicar: "))
        
        if 1 <= n <= 10:
            
            with open(f"tabla-{n}.txt", "w") as file:
                for i in range(1, 11):
                    file.write(f"{n} x {i} = {n * i}\n")
            print(f"Se ha guardado la tabla de multiplicar de {n} en el archivo tabla-{n}.txt")
        else:
            print("El número debe estar entre 1 y 10.")
    
    except ValueError:
        print("Por favor, introduce un número válido.")


def mostrar_tabla_multiplicar():
    try:
        
        n = int(input("Introduce el número entre 1 y 10 para mostrar la tabla: "))
        
        if 1 <= n <= 10:
            
            file_name = f"tabla-{n}.txt"
            if os.path.exists(file_name):
                with open(file_name, "r") as file:
                    print(f"Tabla de multiplicar de {n}:\n")
                    print(file.read())
            else:
                print(f"El archivo {file_name} no existe.")
        else:
            print("El número debe estar entre 1 y 10.")
    
    except ValueError:
        print("Por favor, introduce un número válido.")


def mostrar_linea_tabla():
    try:
        
        n = int(input("Introduce el número de la tabla entre 1 y 10: "))
        m = int(input("Introduce la línea (entre 1 y 10) que deseas ver: "))
        
        if 1 <= n <= 10 and 1 <= m <= 10:
            file_name = f"tabla-{n}.txt"
            if os.path.exists(file_name):
                with open(file_name, "r") as file:
                    
                    lines = file.readlines()
                    if m <= len(lines):
                        print(f"Línea {m} de la tabla de multiplicar de {n}:")
                        print(lines[m - 1]) 
                    else:
                        print(f"La línea {m} no existe en el archivo.")
            else:
                print(f"El archivo {file_name} no existe.")
        else:
            print("Los números deben estar entre 1 y 10.")
    
    except ValueError:
        print("Por favor, introduce números válidos.")


def menu():
    while True:
        print("\nMenú de opciones:")
        print("1. Guardar la tabla de multiplicar de un número")
        print("2. Mostrar la tabla de multiplicar de un número")
        print("3. Mostrar una línea específica de la tabla")
        print("4. Salir")
        
        opcion = input("Selecciona una opción (1-4): ")
        
        if opcion == "1":
            guardar_tabla_multiplicar()
        elif opcion == "2":
            mostrar_tabla_multiplicar()
        elif opcion == "3":
            mostrar_linea_tabla()
        elif opcion == "4":
            print("¡Hasta luego!")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")


menu()