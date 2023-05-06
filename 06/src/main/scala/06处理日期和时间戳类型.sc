
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.{array_contains, coalesce, col, current_date, current_timestamp, date_add, date_sub, datediff, explode, from_json, get_json_object, json_tuple, lit, map, months_between, size, split, struct, to_date, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

//处理日期和时间戳类型
df.printSchema()

//获取当前日期和当前时间戳
val dd =  current_date()
val timestamp = current_timestamp()

val dateDf = spark.range(10).withColumn("today",dd).withColumn("now",timestamp)
dateDf.createOrReplaceTempView("dateTable")

dateDf.show()


//增加或者减去5天
val jian = date_sub(col("today"),5)
val add = date_add(col("today"),5)

dateDf.select(jian,add).show()

spark.sql(s"select date_add(today,5),date_add(today,5) from dateTable")

//两个日期之间的间隔时间
//datediff  返回之间的天数
//months_between 返回之间的月数

dateDf.withColumn("week_ago",date_sub(col("today"),7))
  .select(datediff(col("week_ago"),col("today"))).show(5)

dateDf.select(to_date(lit("2016-01-01")).alias("start"),to_date(lit("2017-05-22")).alias("end")).
  select(months_between(col("start"),col("end"))).show(1)


//to_date() 将字符串转换为日期数据,如果无法解析就返回null

spark.range(5).withColumn("date",lit("2017-01-01")).select(to_date(col("date"))).show(1)

dateDf.select(to_date(lit("2016-20-12")),to_date((lit("2017-12-11")))).show(1)

val dateFormat = "yyyy-dd-MM"
//这版本不支持,dateFormat
//val cleanDteDF = spark.range(1).select(to_date(lit("2017-12-11"),dateFormat).alias("date"))

spark.sql(s"select cast(to_date('2017-01-01') as timestamp)").show()





//控制数据中的空值
//从左到右，遇到第一个非null就返回哪一个。
df.select(coalesce(col("Description"),col("CustomerId"))).show()

//ifnull nullif nvl nvl2


//df.na用来处理null值的
df.na.drop()

//any 存在一个值是null 就删除行
df.na.drop("any")
//所有值都是null 才删除行
df.na.drop("all")

df.na.drop("all",Seq("StockCode","InvoiceNo"))


//fill 函数可以填充null值

//字符串类型
df.na.fill("all null values become this string")

//Integer
df.na.fill(5)
df.na.fill(5)


//好多函数啊，你自己看吧

//null 值的排序
//asc_nulls_first desc_nulls_first asc_nulls_last desc_nulls_last

//处理复杂类型
//结构体，数组,map映射

//结构体

df.selectExpr("(Description,InvoiceNo) as complex","*")

df.selectExpr("struct(Description,InvoiceNo) as complex" , "*")



val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))

complexDF.select("complex.*")

//数组

val array = split(col("Description")," ")

df.select(split(col("Description")," ")).show(2)

df.select(split(col("Description")," ").alias("array_col"))
  .selectExpr("array_col[0]  ").show()

//数组长度
df.select(size(split(col("Description")," "))).show()

df.select(array_contains(array,"WHITE")).show(2)

//这里就是行转列
//explode,就是把数组展开，n*n会变得很多行,7字形
df.withColumn("splitted",array).
  withColumn("exploded",explode(col("splitted"))).
  select("Description","exploded","InvoiceNo").show()

//复习sql的时候，看下lateral view



//map

val maps =    map(col("Description"),col("InvoiceNo"))

df.select(maps).printSchema()

df.select(maps.alias("complex_map")).select(col("complex_map").getItem("Description")).show(2)

df.select(maps.alias("complex_map")).selectExpr("complex_map['Description']").show(2)

df.select(maps.alias("complex_map"))
  .selectExpr("complex_map['xxxxx']").show(2)


//explode  键对值
df.select(maps.alias("complex_map")).selectExpr("explode(complex_map)").show(2)





//处理json类型
val jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

//json 只是一个字符串类型

//如果json只有一层就使用json_tuple查询，多层就使用get_json_object
jsonDF.select(get_json_object(col("jsonString"),"$.myJSONKey.myJSONValue[1]").alias("column")
,json_tuple(col("jsonString"),"myJSONKey")).show(2)


//to_json函数将StructType转换成接送字符串
df.selectExpr("(InvoiceNo,Description) as myStruct").select(to_json(col("myStruct")))


//from_json 把json字符串转化成结构体

val parseSchema = new StructType(Array(
  new StructField("InvoiceNo",StringType,true),
  new StructField("Description",StringType,true)
))

df.selectExpr("(InvoiceNo,Description) as myStruct").
  select(to_json(col("myStruct") as "newJSON")).
  select(from_json(col("newJSON"),parseSchema) ,col("newJSON")).
  show(2)




