import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

val builder:Builder = SparkSession.builder()
builder.appName("Spark examples").master("local[*]")

builder.enableHiveSupport()

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


spark.sql("show databases").show()
spark.sql("show databases").show()
/*spark.catalog.listDatabases().show()
spark.sql("show tables in test").show()
spark.sql("show tables in default").show()*/

