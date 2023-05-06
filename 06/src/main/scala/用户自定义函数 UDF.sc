import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.{col, udf}

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


val udfExampleDF = spark.range(5).toDF("num")
def power3(number:Double):Double = number*number*number
power3(2.0)

udfExampleDF.createOrReplaceTempView("udfTable")

//注册UDF
import org.apache.spark.sql.functions.udf
//这就要看scala语法了，我也不知道为什么是这样写
val power3udf = udf(power3(_:Double):Double)

//使用udf，就像使用其他内置函数一样
udfExampleDF.select(power3udf(col("num"))).show()


//到了这里只能是DF使用这个函数，如果需要sql使用就需要如下操作
spark.udf.register("power3",power3(_:Double):Double)

//我说过selectExpr和sql很相似。selectExpr能用，spark sql也就能用
udfExampleDF.selectExpr("power3(num)").show(3)

spark.sql(s"select power3(num) from udfTable")


spark.sql(s"select power3(2.0)")


//最后使用hive语法来创建udf，那么先启动hive支持

builder.enableHiveSupport()
//end