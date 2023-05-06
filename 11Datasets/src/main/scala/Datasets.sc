import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SparkSession.{Builder, clearActiveSession}

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")


import spark.implicits._

case class Flight(DEST_COUNTRY_NAME:String, ORIGIN_COUNTRY_NAME:String,count:BigInt)

val flightsDF = spark.read.parquet("data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[  Flight]


flights.show(2)

flights.first().DEST_COUNTRY_NAME

def originIsDestination(flight_row: Flight): Boolean = {
  return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}

//Row对象使用起来不好用。


//flightsDF是Row类型的Dataset，Row对象实际上不是很好用，领域对象才是最好用的，比如java使用java对象。
//flightsDF.filter(flight_row => originIsDestination(flight_row)).first()


flights.filter(flight_row => originIsDestination(flight_row)).first()
//collect(）后其实就是原生对象的列表，后面操作就十分好用，就像平时写java代码一样。使用DataSet的好处就是让你感觉在写单机的java代码。
//但是缺点就是会有性能消耗，因为需要将领域对象转换成spark类型。真正执行的是spark类型。
//如果不追求使用方便和类型安全，大可不必使用DataSet，因为py和R语言都是不支持Dataset的。
flights.collect().filter(flight_row => originIsDestination(flight_row))


flights.map(f=>f.DEST_COUNTRY_NAME).take(5)


//连接操作
case class FlightMetadata(count:BigInt,randomData:BigInt)

val flightsMeta = spark.range(500).map(x=> (x,scala.util.Random.nextLong())).withColumnRenamed("_1","count").withColumnRenamed("_2","randomData").as[FlightMetadata]

//Dataset[(flights,flightsMeta)] ,列名是_1，和_2 ,类型是结构体
//flights2: org.apache.spark.sql.Dataset[(Flight, FlightMetadata)] = [_1: struct<DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string ... 1 more field>, _2: struct<count: bigint, randomData: bigint>]
//将两个Dataset
// 整合成一个，每一列都表示一个Dataset
//join的产物是一个元组的Dataset

//注意，我们输出包含一个键值对的Dataset，其中每一行表示一个Flight和Flight元数据
val flights2 = flights.joinWith(flightsMeta,flights.col("count") === flightsMeta.col("count"))

//重点，键值对的Dataset，元组Dataset

//还能这样访问？？？？，666
flights2.selectExpr("_1.DEST_COUNTRY_NAME")

flights2.map(f=>f._1)

flights2.take(2)



//常规的连接,会得到一个丢失了jvm类型信息的DF

//flights2: org.apache.spark.sql.DataFrame = [count: bigint, DEST_COUNTRY_NAME: string ... 2 more fields]
val flights2:DataFrame = flights.join(flightsMeta, Seq("count"))

//转换回去Dataset，需要你另外设置case 类，这就尴尬了，一直使用Dataset不就是一直需要定义case 类？
// flights2.as[sss]

//但它们返回 DataFrame而不是
//Dataset(丢失类型信息)
 flights.groupBy("DEST_COUNTRY_NAME").count()

//Dataset[(String, Long)]
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().show()

//增加了一个叫value的列
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain()

//这种结构看清楚了吗，countryName是分组列，然后迭代器是所有组内的行。
def grpSum(countryName:String, values: Iterator[Flight]):Iterator[(String,Flight)] =  {
  values.dropWhile(_.count < 5).map(x => (countryName,x))
}

//flatMapGroups
//（特定于Scala）将给定函数应用于每组数据。对于每个唯一的组，
// 将向函数传递组密钥和一个迭代器，该迭代器包含组中的所有元素。
// 该函数可以返回包含任意类型元素的迭代器，该元素将作为新的数据集返回。
flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

def grpSum2(f:Flight):Integer ={
  1
}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5)


def sum2(left:Flight, right:Flight) = {
  Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
}

flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l,r) =>sum2(l,r)).take(5)
flights.groupBy("DEST_COUNTRY_NAME").count().explain


