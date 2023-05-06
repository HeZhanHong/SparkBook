import org.apache.spark.{SPARK_REPO_URL, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


spark.sql("SELECT 1 + 1").show()

spark.read.json("data/flight-data/json/2015-summary.json")
  .createOrReplaceTempView("some_sql_view")

//spark.read.json("data/flight-data/json/2015-summary.json").write.saveAsTable("kkk")

//spark.sql("use mydb").show()

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""").write.saveAsTable("kkk2")


//spark.sql("create database mydb").show()
spark.catalog.listDatabases().show()
spark.catalog.listTables().show()


spark.sql("CREATE TABLE flights (\n  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)\nUSING JSON OPTIONS (path 'data/flight-data/json/2015-summary.json')")

spark.catalog.listTables().show()

spark.sql("show databases").show()
spark.sql("use default").show()
spark.sql("show tables").show()