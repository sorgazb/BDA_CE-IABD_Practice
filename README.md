# BDA_CE-IABD_Practice

![Apache Hadoop](https://img.shields.io/badge/Apache_Hadoop-Ecosistema-66ccff?style=for-the-badge&logo=apachehadoop&logoColor=white)&nbsp;![Apache Spark](https://img.shields.io/badge/Apache_Spark-GraphX%20%7C%20RDD-e25a1c?style=for-the-badge&logo=apachespark&logoColor=white)&nbsp;![Apache Hive](https://img.shields.io/badge/Apache_Hive-SQL%20sobre%20Hadoop-fdee21?style=for-the-badge&logo=apachehive&logoColor=black)&nbsp;![Apache Pig](https://img.shields.io/badge/Apache_Pig-Pig%20Latin-f97316?style=for-the-badge)&nbsp;![MapReduce](https://img.shields.io/badge/MapReduce-Procesamiento%20Distribuido-3b82f6?style=for-the-badge)&nbsp;![IES Augustobriga](https://img.shields.io/badge/IES%20Augustobriga-CE%20IABD-6366f1?style=for-the-badge)

> **BDA_CE-IABD_Practice** recoge los ejercicios y proyectos de la asignatura **Big Data Aplicado** del _Curso de Especialización en Inteligencia Artificial y Big Data_ impartido en el **IES Augustóbriga**. Se trabaja con el ecosistema completo de **Apache Hadoop** y herramientas de procesamiento distribuido a gran escala.

---

## 📚 Asignatura

| | |
|---|---|
| **Centro** | IES Augustóbriga |
| **Curso** | C.E. Inteligencia Artificial y Big Data |
| **Asignatura** | Big Data Aplicado |
| **Tecnologías principales** | Hadoop, MapReduce, Hive, Pig, Spark, Sqoop, Flume, Phoenix, GraphX |

---

## 🛠️ Tecnologías del Ecosistema Hadoop

Las herramientas y frameworks trabajados a lo largo de la asignatura:

- **Apache Hadoop & HDFS** — Sistema de ficheros distribuido y gestión de clústeres.
- **MapReduce** — Modelo de programación para procesamiento paralelo de grandes volúmenes de datos.
- **Apache Hive** — Motor de consultas SQL sobre Hadoop para análisis de datos estructurados.
- **Apache Pig** — Plataforma de scripting con Pig Latin para flujos de transformación de datos.
- **Apache Spark** — Motor de procesamiento distribuido en memoria, con soporte batch y streaming.
- **GraphX** — API de Spark para el análisis y procesamiento de grafos a gran escala.
- **Apache Phoenix** — Capa SQL sobre HBase para consultas de baja latencia.
- **Apache Sqoop** — Transferencia de datos entre Hadoop y bases de datos relacionales (RDBMS).
- **Apache Flume** — Ingesta y transporte de datos de log y flujos hacia HDFS en tiempo real.

---

## 🏗️ Estructura del Proyecto

```txt
BDA_CE-IABD_Practice/
└── Ecosistema_Hadoop/
    └── Apache_Spark/
        └── Futbol_RDD/        # Análisis de datos de fútbol con RDDs de Spark
```

> 📂 El repositorio se irá ampliando con ejercicios de Hive, Pig, MapReduce, Sqoop y Flume a medida que avance el curso.

---

## 📌 Proyecto destacado: Futbol\_RDD

Ejercicio práctico con **Apache Spark RDDs** aplicado a un dataset de fútbol. Se trabajan operaciones de transformación y acción sobre RDDs para extraer estadísticas y métricas relevantes del dataset.

**Conceptos aplicados:**
- Creación y manipulación de RDDs con PySpark.
- Transformaciones: `map`, `filter`, `flatMap`, `reduceByKey`.
- Acciones: `collect`, `count`, `take`, `saveAsTextFile`.
- Análisis estadístico de datos deportivos.

---

## ⚙️ Requisitos y Configuración

Clonar el repositorio:
```bash
git clone https://github.com/sorgazb/BDA_CE-IABD_Practice.git
cd BDA_CE-IABD_Practice
```

Este repositorio requiere un entorno Hadoop/Spark configurado. Las opciones recomendadas son:

- **Docker** con imagen oficial de Hadoop/Spark.
- **Clúster local** con Hadoop en modo pseudo-distribuido.
- **Google Colab** con PySpark instalado vía `pip install pyspark`.

Ejemplo con Colab/PySpark:
```bash
pip install pyspark
```

```python
from pyspark import SparkContext
sc = SparkContext("local", "Futbol RDD")
```

---

## 🤝 Contribución

Haz fork del repositorio.

Crea una rama de trabajo:
```bash
git checkout -b feature/mi-nueva-practica
```

Realiza tus cambios y haz commit.

Abre un Pull Request describiendo tus mejoras.

---

<p align="center">
  C.E. Inteligencia Artificial y Big Data &nbsp;·&nbsp; Big Data Aplicado &nbsp;·&nbsp; IES Augustóbriga &nbsp;·&nbsp; Sergio Orgaz Bravo
</p>
