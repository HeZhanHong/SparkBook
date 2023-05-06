import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession.{Builder, clearActiveSession}
import org.apache.spark.util.AccumulatorV2



val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

//使用低级RDD的第二种用法就是分布式共两变量。

//这种分布式共享变量可以在map这些算子上使用。


//驱动器的变量VS分布式共享变量（广播变量）
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
val words = spark.sparkContext.parallelize(myCollection , 2)

val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> -300,"Simple" -> 100)

//注册广播变量
val suppBroadcast = spark.sparkContext.broadcast(supplementalData)

suppBroadcast.value

//关闭变量在map中使用
words.map(word => (word,suppBroadcast.value.getOrElse(word,0))).sortBy(wordPait => wordPait._2).collect()


//累加器
//AccumulatorV2
//蓄电池
case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String, count: BigInt)

//不知为什么不能转换成Dataset
/*val flights = spark.read
  .parquet("/data/flight-data/parquet/2010-summary.parquet")
  .as[Flight]*/


val flights = spark.read
  .parquet("data/flight-data/parquet/2010-summary.parquet")
  .rdd

import org.apache.spark.util.LongAccumulator

val accUnnamed = new LongAccumulator
//先注册累加器
val acc = spark.sparkContext.register(accUnnamed)

//两种方法创建命令累加器
val accChina = new LongAccumulator
spark.sparkContext.register(accChina,"China")

//方法二,创建，注册，一条龙
val accChina2 = spark.sparkContext.longAccumulator("China")


def accChinaFunc(flight_row:Row) ={

  val destination = flight_row.getAs[String]("DEST_COUNTRY_NAME")
  val  origin =  flight_row.getAs[String]("ORIGIN_COUNTRY_NAME")

  if (destination == "China"){
    accChina.add(flight_row.getAs[Long]("count"))
  }
  if (origin == "China"){
    accChina2.add(flight_row.getAs[Long]("count"))
  }
}

flights.foreach(flight_row => accChinaFunc(flight_row))

accChina.value
accChina2.value



//自定义累加器
//尽管Spark确实提供了以西累加器类型
import scala.collection.mutable.ArrayBuffer
val arr = ArrayBuffer[BigInt]()

//[in,out]
class EvenAccumulator extends AccumulatorV2[BigInt,BigInt]{

  private var num:BigInt = 0



  override def isZero:Boolean ={
    this.num == 0
  }

  override def copy() :AccumulatorV2[BigInt,BigInt] = {
    new EvenAccumulator
  }

  override def reset():Unit ={
    this.num = 0
  }

  override def add(v: BigInt):Unit = {
    if (v % 2 == 0){
      this.num += v
    }
  }
  override def merge(other: AccumulatorV2[BigInt, BigInt]):Unit = {
    this.num += other.value
  }

  override def value :BigInt = {
    return  num
  }
}

val acc = new EvenAccumulator
val  newAcc = sc.register(acc,"evenAcc")

acc.value
flights.foreach(flight_row => acc.add(flight_row.getAs[Long]("count")))
acc.value


