import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.{bround, col, corr, expr, initcap, instr, lit, lower, lpad, ltrim, monotonicallyIncreasingId, monotonically_increasing_id, not, pow, regexp_extract, regexp_replace, round, rpad, rtrim, translate, trim, upper}


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


//转换成Spark类型

//会有n行，默认是加了 from dfTable
df.select(lit(5),lit("five"),lit(5.0)).show()

spark.sql(s"select 5, 'five',5.0").show()

//处理布尔类型

df.where(col("InvoiceNo").equalTo(536365)).
  select("InvoiceNo","Description").show(5,false)

//使用col 就特别
df.where(col("InvoiceNo") === 536365).
  select("InvoiceNo","Description").show(5,false)

df.where("InvoiceNo = 536365").show(5,false)

//不等于
df.where("InvoiceNo <> 536365").show(5,false)

val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

//以编程的方式进行,就是记太多方法
df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show()


/*
spark.sql(s"select * from" +
  s" dfTable" +
  s" where Stockode in ('DOT') and " +
  s"(UnitPrice > 600 or instr(Description,'POSTAGE') >= 1 ) ")*/


val DOTCodeFilter = col("StockCode") === "DOT"

df.withColumn("isExpensive" ,
  DOTCodeFilter.and(priceFilter.or(descripFilter)))
  .where("isExpensive == true")
  .show(5)

spark.sql(s"select UnitPrice , (StockCode == 'DOT' and (UnitPrice > 600 or instr(Description,'POSTAGE') >= 1)) as isExpensive from dfTable").show(5)

df.withColumn("isExpensive" , not(col("UnitPrice").leq(250))).
  filter("isExpensive").select("Description","isExpensive").show(5)

df.withColumn("isExpensive",expr("NOT UnitPrice <= 250")).
  filter("isExpensive").select("Description","isExpensive").show(5)


//处理数值的类型
//power //幂运算
val fabricateQuantity = pow(col("Quantity") * col("UnitPrice"),2) + 5
df.select(expr("CustomerId"),fabricateQuantity.alias("realQuantity")).show(2)

//注意字符串中是power，方法中是pow(),他妈的居然不一样
//selectExpr 和sql机器的相似。
df.selectExpr("CustomerId","(power(Quantity * UnitPrice,2.0)+5) as realQuantity").show(2)

spark.sql(s"select CustomerId, (power(Quantity * UnitPrice,2.0)+5) as realQuantity  from dfTable")

//四舍五入

df.select(round(col("UnitPrice"),1).alias("rounded"),col("UnitPrice")).show(5)


df.select(round(lit("2.5")),bround(lit("2.5"))).show(2)

spark.sql(s"select round(2.5),bround(2.5)").show()


//corr 计算两个列的相关性？？
df.stat.corr("Quantity","UnitPrice")
df.select(corr("Quantity","UnitPrice")).show()

spark.sql(s"select corr(Quantity,UnitPrice) from dfTable")

df.describe().show()


//StatFunctions包

val colName = "UnitPrice"
val quantileProbs = Array(0.5)
val relError = 0.05

//DataFrameStatFunctions 这里有很多接口方法用于统计函数，使用df.stat来访问
df.stat

df.stat.approxQuantile("UnitPrice",quantileProbs,relError)
//df.stat.crosstab()
//df.stat.freqItems()

//还有很多的统计函数，我也不知道有什么用。

//为每行添加一个唯一的ID，从0开始

df.select(monotonically_increasing_id()).show(5)

//还有很多方法的，而且还会继续新增。

//处理字符串类型

//大小写转换
//空格分开的英文单词，首字母大写，自己都能实现
df.select(initcap(col("Description"))).show(2,false)

spark.sql(s" select initcap(Description) from dfTable")

df.select(col("Description"),lower(col("Description")),upper(lower(col("Description"))))

spark.sql(s"select Description,lower(Description),upper(lower(Description)) from dfTable ")


//删除字符串走欸的空格或者在其周围添加空格
df.select(
  ltrim(lit("    HELLO    ")).as("ltrim"),
  rtrim(lit("    HELLO    ")).as("rtrim"),
  trim(lit("    HELLO    ")).as("trim"),
  lpad(lit("HELLO"), 3, " ").as("lp"),
  rpad(lit("HELLO"), 10, " ").as("rp")).show(2)



//正则表达式
//regexp_extract,regexp_replace

val simpleColors = Seq("black","white","red","green","blue")
var regexString = simpleColors.map(_.toUpperCase).mkString("|")

// the | signifies `OR` in regular expression syntax

df.select(regexp_replace(col("Description"),regexString,"COLOR").alias("color_clean"),
  col("Description")).limit(2).collect()

spark.sql(s"select regexp_replace(Description,'BLACK|WHITE|GREEN|BLUE','COLOR') as color_clean,Description from dfTable")

//替换
//translate
//使用1337替换LEET
df.select(translate(col("Description"),"LEET","1337"),col("Description")).show(2)

//selectExpr和sql是很相似的
df.selectExpr("translate(Description,'LEET','1337')","Description").show(2)
spark.sql(s"select translate(Description,'LEET','1337'),Description from dfTable").show(2)

//regexp_extract  提取
//不只是hive有，spark也有，sql
regexString = simpleColors.map(_.toUpperCase).mkString("(","|",")")

df.select(regexp_extract(col("Description"),regexString,1).alias("color_clean"),
  col("Description")).show(2)

//包含
//我就不演示了，都一样，就是熟悉这些内置函数
val containsBlack = col("Description").contains("BLACK")
//or 也是返回column....
val containsWhite = col("Description").contains("WHITE").or(containsBlack)

//和上面一样的，哎其实sql是使用这个的
val instrCol  = instr(col("Description"),"BLACK")

//在sql中就是用instr
//放心会留一段时间去熟悉sql和sql函数的
spark.sql(s"select Description from dfTable where instr(Description,'BLACK') >=1 or instr(Description,'WHITE') >= 1").show()


//接下来就很骚的用法，多个

//我就看不懂了
val simpleColors = Seq("black", "white", "red", "green", "blue")
var selectedColumns = simpleColors.map(color => {
  col("Description").contains(color.toUpperCase).alias(s"is_$color")
})
selectedColumns:+expr("*") // could also append this value
df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
  .select("Description").show(3, false)


df.printSchema()