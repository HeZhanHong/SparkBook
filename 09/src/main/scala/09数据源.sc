import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

//数据源，轻松啦没什么难度的

//csv json Parquet orc jdbc 文本文件
//其他数据源，第三方Hbase，xml，MongoDB

//使用DtaFrameReader

//默认使用Parquet格式，是列存储

//读取模式
//保存模式

//csv文件
//读取csv文件

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
val myManualSchema = new StructType(Array(
  new StructField("DEST_COUNTRY_NAME", StringType, true),
  new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  new StructField("count", LongType, false)
))
spark.read.format("csv")
  .option("header", "true")
  .option("mode", "FAILFAST")
  .schema(myManualSchema)
  .load("data/flight-data/csv/2010-summary.csv")
  .show(5)

//错误的数据结构
val myManualSchema2 = new StructType(Array(
  new StructField("DEST_COUNTRY_NAME", LongType, true),
  new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
  new StructField("count", LongType, false) ))

//报错
//For input string: "United States"
/*spark.read.format("csv")
  .option("header", "true")
  .option("mode", "FAILFAST")
  .schema(myManualSchema2)
  .load("data/flight-data/csv/2010-summary.csv")
  .take(5)*/

//写CSV文件

val csvFile =  spark.read.format("csv")
  .option("header", "true")
  .option("mode", "FAILFAST")
  .schema(myManualSchema)
  .load("data/flight-data/csv/2010-summary.csv")

csvFile.write.format("csv").mode("overwrite").option("sep","\t")
  .save("tmp/my-tsv-file.tsv")


//JSON文件
//读取JSON文件
spark.read.format("json").option("mode","FAILFAST").schema(myManualSchema)
  .load("data/flight-data/json/2010-summary.json").show(5)

//写json文件
csvFile.write.format("json").mode("overwrite").save("tmp/my-json-file.json")



//Parquet 文件

//提供列压缩节省空间，而且是列存储。支持按列读取而非读取整个文件。
//是sark默认的文件格式。
//可以存储复杂类型，csv这些是不支持的。

//读取parquet

spark.read.format("parquet").load("data/flight-data/parquet/2010-summary.parquet")
  .show(5)



csvFile.write.format("parquet").mode("overwrite").save("tmp/my-parquet-file.parquet")

//orc文件
//也是parquet一样是容器文件格式，自带数据结构，类型感知的列存储文件格式？
//也是列存储
//orc和parquet非常相似，本质区别是，parquet是针对spark进行了优化，orc是针对hive进行优化

//orc文件

//The ORC data source must be used with Hive support enabled
//spark.read.format("orc").load("data/flight-data/orc/2010-summary.orc").show(5)

//csvFile.write.format("orc").mode("overwrite").save("tmp/my-json-file.orc")


//SQL数据库
//只要支持sql就能和许多系统兼容。

//没有sql数据库，这里就不测试了。

/*val props = new java.util.Properties
props.setProperty("driver", "org.sqlite.JDBC")
val predicates = Array(
  "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
  "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
spark.read.jdbc(url, tablename, predicates, props).show()
spark.read.jdbc(url, tablename, predicates, props).rdd.getNumPartitions // 2*/


//不重合，重复
/*val props = new java.util.Properties
props.setProperty("driver", "org.sqlite.JDBC")
val predicates = Array(
  "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
  "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'")
spark.read.jdbc(url, tablename, predicates, props).count() // 510*/




/*val colName = "count"
val lowerBound = 0L
val upperBound = 348113L // this is the max count in our database
val numPartitions = 10

spark.read.jdbc(url,tablename,colName,lowerBound,upperBound,numPartitions,props)
  .count() // 255*/

//写入文本文件

spark.read.textFile("data/flight-data/csv/2010-summary.csv").show()


  //.selectExpr("split(value, ',') as rows").show()


//写文本文件

csvFile.select("DEST_COUNTRY_NAME").write.mode("overwrite").text("tmp/simple-text-file.txt")

csvFile.select("DEST_COUNTRY_NAME", "count")
  .write.partitionBy("count").mode("overwrite").text("tmp/five-csv-files2.csv")



//并行写数据

//就是多个分区啊，就是重新分区啊

csvFile.rdd.getNumPartitions
csvFile.repartition(5).write.format("csv").mode("overwrite").save("tmp/multiple.csv")

csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME").format("csv")
  .save("tmp/partitioned-files.parquet")


val numberBuckets = 10
val columnToBucketBy = "count"


// 'save' does not support bucketing right now;
//不能使用这个功能啊，一定要spark管理的表

csvFile.write.format("parquet").mode("overwrite")
  .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")

csvFile.write.format("csv").mode("overwrite")
  .bucketBy(numberBuckets, columnToBucketBy).save("tmp/Buckets")









































