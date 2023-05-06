import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{asc, desc, exp, expr, lit, row_number}
import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}



val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

/*
val df = spark.read.format("json")
  .load("data/flight-data/json/2015-summary.json")

df.printSchema()

spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema
*/

val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))

//啥米Metadata？？hello world ??


val df = spark.read.format("json").schema(myManualSchema)
  .load("data/flight-data/json/2015-summary.json")

df.printSchema()
df.schema

//列表达式
import  org.apache.spark.sql.functions.{col,column}
col("someColumn")
column("someColumnName")

//DF被称为不安全类型的api，在编译的时候，DF是不会检查出那些列是不存在的。

df.col("count")

//表达式
expr("someCol")
col("someCol")

expr("someCol -5")
col("someCol") -5
expr("someCol") - 5

df.columns


//记录和行
//记录其实就是Row类型的对象，Row对象也就是记录其实就是字节数组，用户不能字节操作字节数组
//但是可以使用列表达式去操纵

//使用DF时，像驱动器请求行的命令总是返回一个或多个Row类型的行数据
df.first()


//创建行
//Row对象，也就是记录，记录也就是纯纯的值。那么列名字列类型就不要保存在记录上了，
//一万行就重复保存1万份
//列名列对象肯定是保存在DF身上的，也就是表身上，也就是Schema
val myRow = Row("Hello",null,1,false)

myRow(0)
myRow(0).asInstanceOf[String]
myRow.getString(0)
myRow.getInt(2)

//DataFrame转换操作

//创建DataFrame
df.createOrReplaceTempView("dfTable")

val myManualSchema = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))

val myRows = Seq(Row("Hello",null,1L))
val myRDD = spark.sparkContext.parallelize(myRows)
//只是一个RDD[Row]，没有schema没有灵魂
val myDf = spark.createDataFrame(myRDD,myManualSchema)
myDf.show()

//Seq还能这样转换？
//val myDF = Seq(("Hello",2,1L)).toDF("col1","col2","col3")

//select函数和selectExpr函数
df.select("DEST_COUNTRY_NAME").show(2)

spark.sql(s"select DEST_COUNTRY_NAME from dfTable limit 2").show(2)

df.select(col("DEST_COUNTRY_NAME"),col("ORIGIN_COUNTRY_NAME")).show(2)

df.select(
  df.col("DEST_COUNTRY_NAME"),
  col("DEST_COUNTRY_NAME"),
  column("DEST_COUNTRY_NAME"),
  expr("DEST_COUNTRY_NAME")
).show()

//他妈的，还不能一起使用呀
//df.select(col("DEST_COUNTRY_NAME"),"DEST_COUNTRY_NAME")

//expr() 是最灵活的方式

//使用as 改名
df.select(expr("DEST_COUNTRY_NAME as destination")).show(2)

df.select(expr("DEST_COUNTRY_NAME as destination")).alias("xxxxx").show(2)

//select 后面经常跟着一系列的expr，所以专门有一个selectExpr
//sql越来越相似了
df.selectExpr("DEST_COUNTRY_NAME as newColumnName","DEST_COUNTRY_NAME").show(2)

df.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(5)

spark.sql("select * ,(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry from dfTable limit 2")

df.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show(2)
spark.sql(s"select avg(count),count(distinct(DEST_COUNTRY_NAME)) from dfTable limit 2")

//转换操作成Spark类型（字面量） lit literal
//其实哎没什么难度的
//将给定的编程语言上的字面上的值转换操作作为spark可以理解的值

df.select(expr("*"),lit(1).alias("One")).show(2)
spark.sql(s"select * ,1 as One").show(2)


//添加列
df.withColumn("numberOne" , lit(1)).show(2)
spark.sql(s"select * ,1 as numberOne from dfTable limit 2")

df.withColumn("withinCountry",expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME"))

//增加一列，而且值是另外一列的值
df.withColumn("Destination",expr("DEST_COUNTRY_NAME"))


//重命名列
//这里就不会增加列了，就是改名
df.withColumnRenamed("DEST_COUNTRY_NAME","dest").columns

//保留字与关键字

// ` 保留字符，不必转义

//需要转义，但是又想保留字符，就是用 `
import org.apache.spark.sql.functions.expr

//这里的参数不会转义字符，无需使用保留字符
val dfWithLongColName = df.withColumn("This Long Column-Name",expr("ORIGIN_COUNTRY_NAME"))

//这里是输入表达式会转义字符，如果想保留字符就使用`
//有空格的列名比较难处理，与关键字混在一起

dfWithLongColName.selectExpr("`This Long Column-Name`","`This Long Column-Name` as `new col`")

//这里说明的是，col不会转义，expr方法会转义
dfWithLongColName.select(col("This Long Column-Name")).columns
dfWithLongColName.select(expr("`This Long Column-Name`")).columns


//区分大小写
//默认不区分大小写，但是可以使用set spark.sql.caseSensitive true

//删除行
//df.drop("列名","列名2").columns

//更改列的类型（强制类型转换）
//其实就是col的api，从integer转换成long
df.withColumn("count2",col("count").cast("long"))
spark.sql(s"select *,cast(count as long) as count2  from dfTable").columns


//过滤行
df.where(col("count") < 2).show(2)
df.filter("count < 2").show(2)
spark.sql(s"select * from dfTable where count < 2 limit 2")

//where filter 功能一样

//我们可能会想把多个过滤条件写到一个表达式这中，虽然可行，但是并不可行。
//因为Spark会同时执行所有过滤操作，不管过滤条件的先后顺序
//所以最好分开写

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)
spark.sql(s"select * from dfTable where count < 2 and ORIGIN_COUNTRY_NAME != 'Croatia' ").show(2)

//获得去重后的行
//Distinct
//Distinct
df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().show()
spark.sql("select count(distinct ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME) from dfTable").show()



//随机取样
//RDD的取样和这里的取样相比。相同的功能DF就是简单很多
val seed = 5
val withReplacement = false
val fraction = 0.5

//要生成行数的百分比，seed随机因子，withReplacement
df.sample(withReplacement,fraction,seed).count()

//随机分割
//RDD也有相关的api，可以把一个RDD分割成两个RDD

//生成一个数组DF。。。。
val dataFrames = df.randomSplit(Array(0.25,0.75),seed)

dataFrames(0).count()
dataFrames(1).count()
dataFrames(0).count() > dataFrames(1).count()


//连接和追加行（联合操作）
//DF是不可变的，所以不会像数据库表一样添加行。

//如果想DF追加行，那么就将原始的DataFrame与新的DataFrame联合起来,即union操作，也就是拼接两个DataFrame。但是模式必须相同，列数必须相同

//完全相同的模式，和列数，列的位置也要相同
import org.apache.spark.sql.Row
val schema = df.schema
val newRows = Seq(Row("New Country","Other Counntry",5L),Row("New Country 2","Other Counntry 3",1L))
val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDf = spark.createDataFrame(parallelizedRows,schema)

df.union(newDf).where("count = 1 or count = 5").where(col("ORIGIN_COUNTRY_NAME") =!= "United States").show()

//行排序
//sort和order By方法都是相互等价的操作

df.sort("count").show(5)
df.orderBy("count","DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"),col("DEST_COUNTRY_NAME")).show(5)


//明确升序还是降序
df.orderBy(expr("count desc")).show(2)
df.orderBy(desc("count"),asc("DEST_COUNTRY_NAME")).show(2)


//排序要处理null，到底是null排在前面还是后面，这里还有几个异形方法
//asc_nulls_first
//desc_nulls_first
//asc_nulls_last
//desc_nulls_last


//分区内排序并不是全局排序,出于性能优化目的，最好是在进行别的转换之前，先对每个分区进行内部排序。
df.sortWithinPartitions("count")



//limit方法就不必说了吧，都演示了很多遍了

//重划分和合并

//pairRDD就是根据key来分区，这里是根据某一列进行分区。
//如果经常对列进行过滤，如果根据列进行分区后，那么相同的列值就整整齐齐排好，那么过滤性能就变高。


//来了一个重要的优化就是根据一些经常过滤的列对数据进行分区



//控制跨集群数据的物理布局，包括分区方案和分区数

//不管是否有必要，重新分区都会导致数据的全面洗牌.如果将来的分区数大于当前的分区数
//或者你想要基于某一组特定的列进行分区时，通常只能重新分区

df.rdd.getNumPartitions

df.repartition(5)

//根据某一列进行分区
df.repartition(col("DEST_COUNTRY_NAME"))

//添加分区数
df.repartition(5,col("DEST_COUNTRY_NAME"))


//合并操作coalesce
//合并同一节点上的分区，不会进行shuffle 洗牌

df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)



//驱动器获取行
//让驱动器收集一些集群数据到本地，这样就可以让本地机器去处理。

val collectDF = df.limit(10)
collectDF.take(5)
collectDF.show()
collectDF.show(5,false)
collectDF.collect()


//如果数据量特变大，这样做肯定GG，能不能一个一个分区地进行处理？用下面的接口

//返回一个本地迭代器，可以让本机遍历数据集
//将每个分区的数据返回给驱动器
//java.util.Iterator[T]

//一个个分区这样返回？？
//不会一下子所有数据返回？
//这个行数允许你以串行的方式一个一个分区地迭代整个数据集
//如果某一个分区的数据特别大，也照样会使驱动器崩溃。
collectDF.toLocalIterator()






