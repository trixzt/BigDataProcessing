// Databricks notebook source
// MAGIC %md
// MAGIC <h3 style="color:green">Práctica Big Data Processing</h3>

// COMMAND ----------

// MAGIC %md
// MAGIC <h5 style="color:gray">Carga de archivos .CSV</h5>
// MAGIC

// COMMAND ----------

//carga de archivo y ver archivo
// Importar SparkSession
import org.apache.spark.sql.SparkSession

// Crear una instancia de SparkSession
val spark = SparkSession.builder()
  .appName("world report")
  .getOrCreate()

// Cargar el archivo CSV en un DataFrame
val dfPais1 = spark.read.format("csv")
  .option("header", "true")  // Indicar que la primera fila es la fila de encabezado
  .option("inferSchema", "true")  // Inferir automáticamente el esquema de las columnas
  .load("dbfs:/FileStore/practica_final/world_happiness_report.csv")  // Reemplazar "/ruta/al/archivo.csv" con la ruta real de tu archivo CSV

//Mostrar el esquema de dfPais1
display(dfPais1)

// COMMAND ----------

//Ver esquema del archivo
val dfPaisNombre = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/practica_final/world_happiness_report.csv")

//mostrar el esquema
dfPaisNombre.printSchema

// COMMAND ----------

//Cargar el archivo de world_happiness_report_2021.csv

// Importar SparkSession
import org.apache.spark.sql.SparkSession
  
// Crear una instancia de SparkSession
val spark = SparkSession.builder()
  .appName("world report 2021")
  .getOrCreate()

// Cargar el archivo CSV en un DataFrame
val dfPais2021 = spark.read.format("csv")
  .option("header", "true")  // Indicar que la primera fila es la fila de encabezado
  .option("inferSchema", "true")  // Inferir automáticamente el esquema de las columnas
  .load("dbfs:/FileStore/practica_final/world_happiness_report_2021.csv")  // Reemplazar "/ruta/al/archivo.csv" con la ruta real de tu archivo CSV

//mostrar resultado
display(dfPais2021)

// COMMAND ----------

//Ver esquema del archivo world_happiness_report_2021.csv
val dfPaisNombre2021 = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/practica_final/world_happiness_report_2021.csv")

//mostrar resultado
dfPaisNombre2021.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC <h4 style="color:purple">1. ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país)</h4>

// COMMAND ----------

//Importar funciones
import org.apache.spark.sql.functions.{desc,col, max}

//Obtener el país más feliz del 2021
val dfHappyCountry = dfPais2021.orderBy(desc("Ladder score")).select("Country name", "Ladder score").show(1)

// COMMAND ----------

// MAGIC %md
// MAGIC <h4 style="color:purple">2.¿Cuál es el país más “feliz” del 2021 por continente según la data?</h4>
// MAGIC

// COMMAND ----------

//Importar funciones
import org.apache.spark.sql.functions.{first, max}

//Agrupar por region, tomar el valor mas alto de cada region
val dfladderScoreRegion = dfPaisNombre2021.groupBy("Regional indicator").agg(
  first("Country name").as ("Country"),
  max("Ladder score").as("Score"))

//ordenar por Ladder Score  
.orderBy($"Score".desc)  

//Mostrar el resultado
dfladderScoreRegion.show()


// COMMAND ----------

// MAGIC %md
// MAGIC <h5 style="color:gray">En el archivo no se encuentran los continentes, está por región, se requiere saber cuales son esas regiones</h5>
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC <h7 style="color:gray">En esta sección hice un pequeño ejercicio de como podría agregarse una columna para separar por continentes como dice la pregunta</h7>

// COMMAND ----------

//Conocer que regiones se encuentran en el archivo .CSV 
// Seleccionar la columna "Country name" y obtener valores únicos
val dfWichRegion = dfPaisNombre2021.select("Regional indicator").distinct()

// Mostrar los nombres las regiones
display(dfWichRegion)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5 style="color:gray">Al obtener cada región, ahora la tengo que relacionar con su País</h5> 

// COMMAND ----------

//Hacer una columna para agregar los continentes
import org.apache.spark.sql.functions._

// Definir una función para asignar continentes según la región
def continenteXPais(region: String): String = {
  region match {
    case "South Asia" => "Asia"
    case "Middle East and North Africa" => "Africa"
    case "North America and ANZ" => "Oceania"
    case "Sub-Saharan Africa" => "Africa"
    case "East Asia" => "Asia"
    case "Commonwealth of Independent States" => "Asia"
    case "Latin America and Caribbean" => "America"
    case "Western Europe" => "Europe"
    case "Central and Eastern Europe" => "Europe"
    case "Southeast Asia" => "Asia"
  }
}

// Crear una nueva columna "Continent" que tome de referencia Regional Indicator
val dfContinentesUDF = udf(continenteXPais _)

//Nueva columna Continent
val dfPaisConContinente = dfPaisNombre2021.withColumn("Continent", dfContinentesUDF(col("Regional indicator")))

// Resultado
display(dfPaisConContinente)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5 style="color:gray">Verificar que estén alineados el país, región y continente</h5>
// MAGIC
// MAGIC

// COMMAND ----------

// Seleccionar la columna Country name, Regional indicator, Continent
val regionContinente = dfPaisConContinente.select("Country name","Regional indicator","Continent")

// Mostrar tabla
display(regionContinente)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5 style"color:gray">Obtener el país más féliz por Continente</h5>

// COMMAND ----------

//Obtener el índice más alto por país según el continente
val ladderScoreRegion = dfPaisConContinente.groupBy("Continent").agg(
  first("Country name").as ("Pais"),
  max("Ladder score").as("Score"))
   
//Mostrar resultado   
display(ladderScoreRegion)

// COMMAND ----------

// MAGIC %md
// MAGIC <h4 style="color:purple">3.¿Cuál es el país que más veces ocupó el primer lugar en todos los años?</h4>

// COMMAND ----------

// MAGIC %md
// MAGIC <h5 style"color:gray">Agregar una columna de año al archivo del 2021</h5>

// COMMAND ----------

//Agregar una columna con el año 2021
val dfYearAdd2021 = dfPaisNombre2021.select($"Country name", lit(2021).as("year"), $"Ladder score",$"Logged GDP per capita".as("Log GDP per capita"),$"Healthy life expectancy".as("Healthy life expectancy at birth"))

//Mostrar resultado
dfYearAdd2021.show(5)

// COMMAND ----------

//Obtener otra tabla con los datos que sólo se requieren y cambiar life ladder por ladder score
val dfAllYears = dfPais1.select($"Country name", $"year", $"Life Ladder".as("Ladder score"),$"Log GDP per capita",$"Healthy life expectancy at birth")

//dfAllYears.show(5)
display(dfAllYears)

// COMMAND ----------

//juntar las dos tablas de los dos archivos .CSV
val dfCountryUnion = dfAllYears.unionByName(dfYearAdd2021)

//Resultado
display(dfCountryUnion)

// COMMAND ----------

//Resultado más alto de Liefe Score por año
// Ordenar de menor a mayor considerando year y Ladder score
val dfgroupCountry= dfCountryUnion.orderBy(col("year"), col("Ladder score").desc)

// Seleccionar el primer país en el ranking para cada año.
val dfcountryByYear = dfgroupCountry.groupBy("year")
      .agg(
        first("Country name").as ("Countries"),
        max("Ladder score").as("Life Scores")
        )

// Resultado
dfcountryByYear.show()


// COMMAND ----------

//Contar la cantidad de veces que un País es no.1
import org.apache.spark.sql.functions.{first, max}

//Agrupar por pais, contar las veces que aparece
val dfCountryHappy = dfcountryByYear.groupBy("Countries")
  .agg(count("*").alias("count"))

//ordenar por veces en que más aparece  
.orderBy($"count".desc)  

//Mostrar el resultado
dfCountryHappy.show(2)

// COMMAND ----------

// MAGIC %md
// MAGIC <h4 style="color:purple">4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?</h4>

// COMMAND ----------

//Filtrar por año 2020, ordenarlos, agregar ranking y obtener el más alto
//Ordenar los Países por Ladder score y agregar una columna con el ranking de felicidad según el año 2020
import org.apache.spark.sql.expressions.Window

// Definir la ventana de análisis
val windowOrderRanking = Window.partitionBy("year").orderBy(col("Ladder score").desc)

// Filtrar los datos para obtener solo los del año 2020
val dfYear2020 = dfCountryUnion.filter($"year" === 2020)
  .orderBy(col("Ladder score").desc)
  .withColumn("Ranking", rank().over(windowOrderRanking))

//Mostrar los países con el ranking
dfYear2020.show()

// COMMAND ----------

//Obtener el país con mayor GDP

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Obtener el mayor GDP
val mayorGDP = dfYear2020.orderBy(col("Log GDP per capita").desc).select("Ranking","Country name", "year", "Ladder score","Log GDP per capita").limit(1)

// Mostrar el resultado
mayorGDP.show()

// COMMAND ----------

// MAGIC %md
// MAGIC <h4 style="color:purple">5.¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?</h4>

// COMMAND ----------

//Saber si el nivel de GDP aumentó o disminuyó

//Ordenar los Países por Ladder score y agregar una columna con el ranking de felicidad según el año 2020
import org.apache.spark.sql.functions._

// Filtrar los datos para obtener solo los años 2020 y 2021, y quitar los valores nulos en la columna de GDP
val dfYear2020 = dfCountryUnion.filter($"year" === 2020 && $"Log GDP per capita".isNotNull)
val dfYear2021 = dfCountryUnion.filter($"year" === 2021 && $"Log GDP per capita".isNotNull)

// Calcular el GDP promedio para cada año
val dfAvg2020 = dfYear2020.agg(round(avg("Log GDP per capita"),3)).collect()(0).getDouble(0)
val dfAvg2021 = dfYear2021.agg(round(avg("Log GDP per capita"),3)).collect()(0).getDouble(0)

// Calcular el porcentaje de variación entre los dos años
val dfDifferencePercentage = ((dfAvg2020 - dfAvg2021) / dfAvg2020) * 100

//Conocer si incrementó o disminuyó
if (dfDifferencePercentage > 0) {
  println(s"GDP percentage increased $dfDifferencePercentage %")
} else if (dfDifferencePercentage < 0) {
  println(s"The percentage of GDP decreased $dfDifferencePercentage %")
} else {
  println(s"GDP percentage remained the same $dfDifferencePercentage %")
}

// COMMAND ----------

// MAGIC %md
// MAGIC <h4 style="color:purple">6.¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia
// MAGIC en ese indicador en el 2019?</h4>

// COMMAND ----------

//Obtener el País con mayor expectativa de vida

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Obtener el Healthy life expectancy at birth de cada País
val mayorHealthyLife1 = dfCountryUnion.orderBy(col("Healthy life expectancy at birth").desc).select("Country name", "year", "Healthy life expectancy at birth")

// Mostrar el resultado
mayorHealthyLife1.show()

// COMMAND ----------

// Ordenar de menor a mayor considerando year y Ladder score
val dfGroupCountryHealthy= dfCountryUnion.orderBy(col("year"), col("Healthy life expectancy at birth").desc)

// Seleccionar el primer país en el ranking para cada año.
val dfGroupByHealthy = dfGroupCountryHealthy.groupBy("year")
      .agg(
        first("Country name").as ("Countries"),
        max("Healthy life expectancy at birth").as("Healthy life expectancy at birth")
        )
//Cantidad de países por año
dfGroupByHealthy.show()


// COMMAND ----------

//Contar la cantidad de veces que un País es no.1 y sacar el promedio

import org.apache.spark.sql.functions.{first, max}

//Agrupar por pais, contar las veces que aparece
val dfCountryHealthy = dfGroupByHealthy.groupBy("Countries")
 .agg(round(avg("Healthy life expectancy at birth")).alias("AVG_Healthy_life_expectancy"))

// Ordenar por el promedio de esperanza de vida saludable de mayor a menor
val dfMaxAvgCountry = dfCountryHealthy.orderBy(desc("AVG_Healthy_life_expectancy"))

// Mostrar el país con el máximo valor del promedio de esperanza de vida saludable
dfMaxAvgCountry.show()

// COMMAND ----------

val topCountryHealthy = dfMaxAvgCountry.select($"Countries").first().getString(0)
val topAvghealthy = dfMaxAvgCountry.select($"AVG_Healthy_life_expectancy").first().getDouble(0)

println(s"The Country with the most highest Healthy life expectancy at birth is $topCountryHealthy, with an average of Healthy life expectancy at birth $topAvghealthy years old" )

// COMMAND ----------

//Obtener el pais con valor más alto de Healthy life expectancy at birth en 2019
// Filtrar mayorHealthyLife1 para obtener solo los datos del año 2019 y el país obtenido en topCountryHealthy
val countryHealthy2019 = dfGroupByHealthy
  .filter($"year" === 2019 && $"Countries" === topCountryHealthy)
  .select("year","Countries","Healthy life expectancy at birth")

// Mostrar resultado
countryHealthy2019.show()
