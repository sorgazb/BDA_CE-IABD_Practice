from pyspark import SparkContext, SparkConf

# Configurar Spark
conf = SparkConf().setAppName("Análisis Fútbol")
sc = SparkContext(conf=conf)

# 1️⃣ Cargar y unir archivos
liga1 = sc.textFile("liga1.txt")
liga2 = sc.textFile("liga2.txt")
todas_las_ligas = liga1.union(liga2)

print("=== TODOS LOS PARTIDOS ===")
for partido in todas_las_ligas.collect():
    print(partido)

# 2️⃣ Obtener todos los equipos
equipos = todas_las_ligas.flatMap(lambda linea: linea.split(",")[:2])

# 3️⃣ Contar equipos distintos
equipos_distintos = equipos.distinct()
num_equipos = equipos_distintos.count()
print(f"\n=== EQUIPOS DISTINTOS: {num_equipos} ===")

# 4️⃣ Partidos donde el local marcó más de 1 gol
partidos_goles_local = todas_las_ligas.filter(
    lambda linea: int(linea.split(",")[2]) > 1
)
print("\n=== PARTIDOS CON MÁS DE 1 GOL LOCAL ===")
for partido in partidos_goles_local.collect():
    print(partido)

# 5️⃣ Total de goles
total_goles = todas_las_ligas.map(
    lambda linea: int(linea.split(",")[2]) + int(linea.split(",")[3])
).reduce(lambda a, b: a + b)
print(f"\n=== TOTAL DE GOLES: {total_goles} ===")

# 6️⃣ Primeros 3 partidos
primeros_3 = todas_las_ligas.take(3)
print("\n=== PRIMEROS 3 PARTIDOS ===")
for partido in primeros_3:
    print(partido)

# 7️⃣ Recoger equipos distintos
print("\n=== TODOS LOS EQUIPOS ===")
for equipo in equipos_distintos.collect():
    print(equipo)

# 8️⃣ Guardar en disco
equipos_distintos.saveAsTextFile("equipos_futbol")
print("\n=== EQUIPOS GUARDADOS EN equipos_futbol/ ===")

# 🏆 EXTRA: Goles totales por equipo
goles_local = todas_las_ligas.map(
    lambda linea: (linea.split(",")[0], int(linea.split(",")[2]))
)
goles_visitante = todas_las_ligas.map(
    lambda linea: (linea.split(",")[1], int(linea.split(",")[3]))
)
goles_totales = goles_local.union(goles_visitante).reduceByKey(lambda a, b: a + b)

print("\n=== GOLES TOTALES POR EQUIPO ===")
for equipo, goles in goles_totales.collect():
    print(f"{equipo}: {goles} goles")

# Equipo más goleador
equipo_mas_goleador = goles_totales.sortBy(lambda x: x[1], ascending=False).first()
print(f"\n=== EQUIPO MÁS GOLEADOR: {equipo_mas_goleador[0]} con {equipo_mas_goleador[1]} goles ===")

# Cerrar contexto
sc.stop()
