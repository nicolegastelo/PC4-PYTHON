
with open('/workspaces/PC4-PYTHON/temperaturas(1).txt', 'r') as file:
    lines = file.read()

temperaturas = []
fechas = []

for line in lines:
    fecha, temperatura = line.strip().split(',')
    fechas.append(fecha)
    temperaturas.append(float(temperatura))

temp_promedio = sum(temperaturas) / len(temperaturas)
temp_maxima = max(temperaturas)
temp_minima = min(temperaturas)

with open('resumen_temperaturas.txt', 'w') as output_file:
    output_file.write(f'Temperatura Promedio: {temp_promedio:.2f}°C\n')
    output_file.write(f'Temperatura Máxima: {temp_maxima:.2f}°C\n')
    output_file.write(f'Temperatura Mínima: {temp_minima:.2f}°C\n')

print("El resumen de las temperaturas ha sido guardado en 'resumen_temperaturas.txt'")
