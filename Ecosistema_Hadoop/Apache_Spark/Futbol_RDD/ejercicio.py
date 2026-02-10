from pyspark import SparkContext, SparkConf

configuracion = SparkConf().setAppName("Análisis Fútbol")
sc = SparkContext(conf=configuracion)

liga1 = sc.textFile("liga1.txt")
liga2 = sc.textFile("liga2.txt")
ficherosResultados = liga1.union(liga2)

# De cada linea del fichero union obtenemos los equipos, dos primeros
# elementos de la fila
equiposPartidos = ficherosResultados.flatMap(lambda partido: partido.split(",")[:2])
print('Lista de equipos que han participado: ')
equiposPartidos.foreach(print)

# Y para obtener el numero de equipos distintos 
# usamos count
numeroEquipos = equiposPartidos.distinct().count()
print(f'\nTotal equipos distintos: {numeroEquipos}')

# 
partidosLocalGoles= ficherosResultados.filter(
    lambda partido: int(partido.split(",")[2]) > 1
)
print('\nLista de partidos con mas de 1 gol local: ')
partidosLocalGoles.foreach(print)

# 
totalGoles = ficherosResultados.map(
    lambda partido: int(partido.split(",")[2]) + int(partido.split(",")[3])
).reduce(lambda golesLocal, golesVisitante: golesLocal + golesVisitante)
print(f'\nTotal de goles: {totalGoles}')

# 
primerosTresPartidos = ficherosResultados.take(3)
print('\nPrimeros 3 partidos: ')
for partido in primerosTresPartidos:
    print(partido)

# Para obtener todos los equipos distintos utilizamos
# la funcion distinct
equipos = equiposPartidos.distinct()
print('Lista de equipos distintos que han participado: ')
equipos.foreach(print)

# Ejercicios Extra

golesLocal = ficherosResultados.map(
    lambda partido: (partido.split(",")[0], int(partido.split(",")[2]))
)
golesVisitantes = ficherosResultados.map(
    lambda partido: (partido.split(",")[1], int(partido.split(",")[3]))
)
totalGolesEquipo = golesLocal.union(golesVisitantes).reduceByKey(lambda total, goles: total + goles)
print('\nTotal goles equipo: ')
totalGolesEquipo.foreach(lambda equipo: print(f'{equipo[0]}: {equipo[1]} goles.'))


equipoMasGoles = totalGolesEquipo.sortBy(lambda equipo: equipo[1], ascending=False).first()
print(f'\nEl equipo con mas goles es: {equipoMasGoles[0]} con {equipoMasGoles[1]} goles')

equipos.saveAsTextFile("equipos_futbol")

sc.stop()
