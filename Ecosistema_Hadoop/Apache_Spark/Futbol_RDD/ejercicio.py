from pyspark import SparkContext, SparkConf

# Configuracion de PySpark
configuracion = SparkConf().setAppName("Análisis Fútbol")
sc = SparkContext(conf=configuracion)

# Cargamos los ficheros de texto con los resultados
liga1 = sc.textFile("liga1.txt")
liga2 = sc.textFile("liga2.txt")
# Y realizamos la union
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

# Para obtener los partidos en los que el equipo local 
# ha metido mas de 1 gol usamos un filter, que devolvera,
# aquellos partidos que cumplan la condicion.
partidosLocalGoles= ficherosResultados.filter(
    lambda partido: int(partido.split(",")[2]) > 1
)
print('\nLista de partidos con mas de 1 gol local: ')
partidosLocalGoles.foreach(print)

# Para obtener el total de goles que hay en el fichero
# usamos un map para optener todos los valores y un reduce
# para obtener la suma de los goles de cada equipo
totalGoles = ficherosResultados.map(
    lambda partido: int(partido.split(",")[2]) + int(partido.split(",")[3])
).reduce(lambda golesLocal, golesVisitante: golesLocal + golesVisitante)
print(f'\nTotal de goles: {totalGoles}')

# Para obtener los 3 primeros partidos usamos un take
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

# Para obtener los goles totales de cada equipo primero debemos hacer
# 2 maps uno para los goles como local y otro para los goles de visitantes
golesLocal = ficherosResultados.map(
    lambda partido: (partido.split(",")[0], int(partido.split(",")[2]))
)
golesVisitantes = ficherosResultados.map(
    lambda partido: (partido.split(",")[1], int(partido.split(",")[3]))
)
# Una vez obtenidos los goles de visitante y de local hacemos un union y aplicamos
# un reduce por la Key, que es el nombre del equipo y vamos acumulando el total de goles
totalGolesEquipo = golesLocal.union(golesVisitantes).reduceByKey(lambda total, goles: total + goles)
print('\nTotal goles equipo: ')
totalGolesEquipo.foreach(lambda equipo: print(f'{equipo[0]}: {equipo[1]} goles.'))

# Para obtener el equipo con mas goles ordenamos el array del ejercicio anterior
# y obtenemos el primer valor
equipoMasGoles = totalGolesEquipo.sortBy(lambda equipo: equipo[1], ascending=False).first()
print(f'\nEl equipo con mas goles es: {equipoMasGoles[0]} con {equipoMasGoles[1]} goles')

equipos.saveAsTextFile("equipos_futbol")

sc.stop()
