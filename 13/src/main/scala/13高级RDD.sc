import org.apache.hadoop.mapred.lib.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DatasetHolder, SparkSession}
import org.apache.spark.sql.SparkSession.Builder

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
val words = spark.sparkContext.parallelize(myCollection,2)

//map
words.map(word => (word.toLowerCase,1))
//keyBy
val keyword = words.keyBy(word => word.toLowerCase().toSeq(0).toString)

//对值进行映射
//只对value执行修改，不修改key
keyword.mapValues(word => word.toUpperCase()).collect()

//不需要转换成字符数组的？
keyword.flatMapValues(word => word.toUpperCase()).collect()

//提取key和value
keyword.keys
keyword.values


//lookup
//查找
//通过key查找value
keyword.lookup("s")

//sampleByKey 通过key取样。
//精准取样和近似取样
val distinctChars = words.flatMap(word => word.toLowerCase().toSeq).distinct().collect()
import  scala.util.Random
val sampleMap = distinctChars.map(c => (c,new Random().nextDouble())).toMap

//大约
words.map(wold => (wold.toLowerCase().toSeq(0),wold)).sampleByKey(true,sampleMap,6L).collect
//99.99%置信度,精准
words.map(wold => (wold.toLowerCase().toSeq(0),wold)).sampleByKeyExact(true,sampleMap,6L).collect

//聚合操作
val chars = words.flatMap(word => word.toLowerCase.toSeq)
val KVcharacters =  chars.map(letter => (letter,1))
def maxFunc (left : Int, right : Int) = math.max(left,right)
def addFunc(left : Int, right : Int) = left+right
val nums = sc.parallelize(1 to 30 ,5)
nums.foreachPartition(iter =>
  {
    println("sss")
    iter.foreach(f => println(f))
  }
 )


val timeout = 1000L
val confidence = 0.95
KVcharacters.countByKey()
KVcharacters.countByKeyApprox(timeout,confidence)

//了解聚合操作的实现
//reducebykey因为已经预先知道聚合函数所以会在shuffle前对分区内进行预聚合。
//groupbykey只是把key分组，不知道聚合函数所以无法进行预聚合。
//一般是使用reducebykey()
KVcharacters.groupByKey().map(row => (row._1,row._2.reduce(addFunc))).collect()

KVcharacters.reduceByKey(addFunc).collect()

//其他聚合方法
//aggregate
//两个函数，分区内聚合，分区间聚合
//不是kvRDD？
//返回只有一个值
nums.aggregate(0)(maxFunc,addFunc)

//kvRDD
KVcharacters.aggregateByKey(0)(addFunc,maxFunc)

//如果KVcharacters经过分组，那么分区间聚合函数是无意义的，因为key只存在一个分区中。

//combineByKey
//不但可以指定聚合函数，还可以指定一个合并函数

val valToCombiner = (value:Int) => List(value)
val mergeValuesFunc = (vals:List[Int],valToAppend:Int) => valToAppend::vals
val mergeCombinerFunc = (vals1:List[Int],vals2:List[Int]) =>vals1::vals2

val outputPartitions = 6
/*KVcharacters
  .combineByKey(
    valToCombiner,
    mergeValuesFunc,
    mergeCombinerFunc,
    HashPartitioner
    )
  .collect()*/


//foldBykey

KVcharacters.foldByKey(0)(addFunc).collect()

//CoGroups
import scala.util.Random
val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
charRDD.cogroup(charRDD2, charRDD3).take(5)

//连接操作
//RDD的连接操作没几个接口

//内连接
val  keyedChars = distinctChars.map( c => (c,new Random().nextDouble()))
val outputPartitions = 10
KVcharacters.join(keyedChars).foreach( item =>
  {
    println(item._1 + " " + item._2._1 + " "+ item._2._2)
  }
)
KVcharacters.join(keyedChars).collect()


//还有很多其他的连接,都是一样的格式
//全外连接，左外连接，右外连接，笛卡尔连接


//zip
//将两个RDD结合再一起
//两个普通RDD，组成一个pair rdd，需要条数和分区数都要相等。前面的rdd作为ke，后面的rdd作为value

val numRange = sc.parallelize(0 to 9 ,2)
words.zip(numRange).collect()

//控制分区
//使用RDD，你可以控制数据再整个集群中的物理分布.结构化API中不支持的。
//coalesce
//一个节点上有多个分区，这个api可以折叠同一个节点上的分区。、
//减少了网络传输，减少了shuffle
words.coalesce(1).getNumPartitions


//repartition
//重新分区
//重新分区，跨节点的分区会执行shuffle操作。对于map和filter操作，增加等去可以提高并行度。
//提高分区，那么意味着一个分区会分开多个分区，那么就是shuffle的操作。
//虽然分区器是一样，分区数改变（增多不必说，减少其实也会），都会产生shuffle，需要发挥想象力。


//repartitionAndSortWithinPartitions
//书本上只是提了下，应该和repartition差不多


//自定义分区
//自定义分区是使用RDD的主要原因之一，结构化Api不支持自定义数据分区。

//简而言之，自定义分区的唯一目的就是将数据均匀地分布到整个集群中，以避免诸如数据倾斜之类的问题。

//实现Partitioner的子类。
//只有当你很了解特定领域知识时,你才需要这样做。

//file:/D:/BigDataSoft/IntelliJ IDEA 2021.2.3/jbr/bin/data/retail-data/all
val df = spark.read.option("header","true").option("inferSchema","true").csv("data/retail-data/all/")
val rdd = df.coalesce(10).rdd
rdd.collect()

//俩个默认内置分区器，HashPartitioner和RangePartitioner（根据数值范围分区）
//离散值就用hash，连续值就用Range

import org.apache.spark.HashPartitioner
rdd.map(r => r(6)).take(5).foreach(println)
val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)

//普通RDD没有分区器？
//rdd.partitioner



import org.apache.spark.Partitioner

class DomainPartitioner extends Partitioner {

  //只有3个分区，0，1，2
  def numPartitions = 3

  override def getPartition(key: Any):Int ={

    val customerId = key.asInstanceOf[Double].toInt
    if (customerId == 17850.0 || customerId == 12583.0){
      return 0;
    }else{
      //只会随机到 1和2
      return new java.util.Random().nextInt(2) + 1
    }
  }
}

keyedRDD.partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length).take(5)
//keyedRDD.partitioner



//自定义序列化
//如果希望并行处理（或函数操作）的对象都必须是可序列化
//kryo 序列化，avro序列化



//再工作节点之间的数据传输时或将RDD写到磁盘中。

//那么怎么自定义序列化类？书本上没说，应该都是继承Serializable
//然后注册使用



//如果设置并设置“spark.serializer”为
//“org.apache.spark.serializer.KryoSerializer”
//使用kryo序列化时候，spark2.0.0后，简单类型已经默认注册了
//简单类型，简单类型数组或字符串类型的RDD，就不需要注册了
//如果使用自定义的类，那么就需要注册。

//使用kryo需要先注册
/*val conf = new SparkConf()
conf.registerKryoClasses()
conf.registerAvroSchemas()
val sc = new SparkContext(conf)*/






