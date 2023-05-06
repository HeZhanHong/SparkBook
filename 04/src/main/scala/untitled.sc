import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

val  spark : SparkSession =SparkSession.builder().appName("Spark example").master("local[*]").getOrCreate()
//builder.appName("Spark example").master("local[*]")

//val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


val  logger  =   Logger.getLogger("org")

//类型为bigint的df
val df = spark.range(500).toDF("number")


val df2 = df.select(df.col("number") + 10)
df2.show()
