import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.desc

import scala.reflect.internal.util.TableDef.Column

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


val  logger  =   Logger.getLogger("org")

val myRange: DataFrame =  spark.range(1000).toDF("number")

val divisBy2 = myRange.where("number % 2 = 0")

divisBy2.count()
divisBy2.show()

//val path = new Path("/data/flight-data/csv/2015-summary.csv")

val flightData2015 = spark.read.
  option("inferSchema","true").
  option("header",true).
  csv("E:\\workFile\\Spark-book\\data\\flight-data\\csv\\2015-summary.csv")

flightData2015.explain()


 flightData2015.take(3).foreach(row => logger.warn(row.toString()))

flightData2015.sort("count").explain()

spark.conf.set("spark.sql.shuffle.partitions",5)

printf("系统输出？")

flightData2015.sort("count").explain()

flightData2015.createOrReplaceTempView("flight_data_2015")


val sqlWay =  spark.sql("SELECT DEST_COUNTRY_NAME, count(1)  FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME")


val sqlWay2 = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

sqlWay.explain()

val dataFrameWay = flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .count()

dataFrameWay.explain()

spark.sql("select max(count) from flight_data_2015").take(1)

import org.apache.spark.sql.functions.max
flightData2015.select(max("count")).take(1)


//order by 是在select 后面执行的，所以可以使用列别名 limit 是最后执行
//执行顺序可以看DF风格，完美按照执行顺序运行
val maxsql = spark.sql(
 """select DEST_COUNTRY_NAME,sum(count) as destination_total
   |from flight_data_2015
   |group by DEST_COUNTRY_NAME
   |ORDER by destination_total DESC
   |limit 5
   |""".stripMargin)

maxsql.show()

val df =  flightData2015.groupBy("DEST_COUNTRY_NAME").
  sum("count").
  withColumnRenamed("sum(count)","destination_total").
  sort(desc("destination_total"))
  .limit(5)

df.show()
df.explain()
