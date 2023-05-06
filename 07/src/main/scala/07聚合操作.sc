import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.{approx_count_distinct, col, collect_list, collect_set, countDistinct, sum}

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


//聚合操作
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("data/retail-data/all/*.csv")
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")

//这个是一个action算子
df.count()

import org.apache.spark.sql.functions.count
//聚合函数
val  ss : DataFrame =  df.select(count(col("StockCode")))
ss.show()

//spark.sql返回的是df类型，明显是一个转换算子
spark.sql(s" select count(1) from dfTable").show()

//
//count(*) 会对null值计算，count(1) 不会对null值计算

//countDistinct
df.select(countDistinct("StockCode")).show()

spark.sql(s" select count(distinct StockCode) from dfTable").show()


//approx_count_distinct 大约值,参数2 可容忍的最大误差，越少准确率越高。
df.select(approx_count_distinct("StockCode",0.1)).show()

spark.sql(s" select approx_count_distinct(StockCode,0.1) from dfTable ").show()

//还有
//first
//last
//min
//max
//sum sumDistinct
//avg mean
//都是聚合函数，都是一样的用法

//方法和标准差
//还有好多数学函数，数学原理


//聚合输出复杂类型
//收集某一个列的值到一个list列表中，或者将unique唯一值收集到一个set集合里，这也是聚合函数啊

df.agg(collect_set("Country"),collect_list(col("Country"))).show()

//def agg(expr: Column, exprs: Column*): DataFrame = groupBy().agg(expr, exprs : _*)
//groupby()之后就是喜欢加agg()

//foreach(f=> println(f))

//分组
//如果是数据表这种结构，当然就是根据列进行分组，kvRDD这种就是根据key进行分组。

//第一步返回一个RelationalGroupedDataset，第二步返回DataFrame。
//RelationalGroupedDataset不是DF，不能直接show
df.groupBy("InvoiceNo","CustomerId").count().show()


//使用表达式分组
//有空再看下这里吧，和前面的不一样


df.groupBy("InvoiceNo").
  agg(count(col("Quantity")).alias("quan"),functions.expr("count(Quantity)")).show()


//使用Map进行分组
//键为列，值为要执行的字符串形式的聚合函数

df.groupBy("InvoiceNo").
  agg("Quantity"->"avg","Quantity"->"stddev_pop").show()

spark.sql(s"select avg(Quantity),stddev_pop(Quantity),InvoiceNo from dfTable group by InvoiceNo")


df.rdd.aggregate()


//后面这些需要sql知识，分析函数

//window函数
//分组集
//用户自定义聚合函数
