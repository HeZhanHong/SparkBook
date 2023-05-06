import org.apache.avro.file.BZip2Codec
import org.apache.spark.{SPARK_BRANCH, SparkContext}
import org.apache.spark.sql.{DatasetHolder, SparkSession}
import org.apache.spark.sql.SparkSession.Builder

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

//创建RDD

//在DataFrame，Dataset和RDD之间进行交互操作
spark.sparkContext
spark.range(500).rdd

//此时是row类型的Rdd
//Dataset[long]=>DF[Row](Dataset[Row])=>RDD[Row]
spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

//需要隐性转换，RDD的Long类型,为什么这里不行的？
//spark.range(10).rdd.toDF()


//从本地集合中创建RDD
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
//ParallelCollectionRDD 并行采集RDD
val words = spark.sparkContext.parallelize(myCollection,2)

words.setName("myWords")
words.name
words.collect().foreach(row => println(row))
//从数据源创建
//HadoopRDD
//spark.sparkContext.textFile("/some/path/withTextFiles")
//一个文件的内容为一行，看注释啊
//WholeTextFileRDD
//整体，全文
//spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")


//操作RDD
//RDD的操作对象是java对象而不是spark类型，以及缺乏一些辅助方法

//转换操作
//可以看到这个distinct 是会运行reduceBykey 运行shuffleRDD的
words.distinct().count()
//filter
//过滤
def startsWithS(indicidual:String) = {
  indicidual.startsWith("S")
}

words.filter(word => startsWithS(word))
words.map(word => (word,word(0),word.startsWith("S")))

words.flatMap(word => word.seq).collect()
words.sortBy(word => word.length * -1 )

//随机分割
//将一个RDD随机切成若干个RDD，返回一个RDD的列表
val fiftyFiftySplit = words.randomSplit(Array[Double](0.5,0.5))

//动作操作
spark.sparkContext.parallelize(1 to 20).reduce(_+_)

//获单词最长的单词
//你看这这个需求就不是聚合的应用了吧。你要使用这些算子去满足你的需求。
def wordLenghtReducer(leftWord:String,rightWord:String) :String ={
  if(leftWord.length > rightWord.length){
    return leftWord
  }else
    return rightWord
}

words.reduce(wordLenghtReducer)
//使用
words.count()

words.countApprox(400,0.95)
//近似的，看spark中的解析的。
//之所以用近似不需要太准确，但是执行速度快。
words.countApproxDistinct(0.05)

//就是返回map键对值。如果数据量比较大这个返回就很大，可以使用reduceBykey来实现啊。
words.countByValue()
//Map(Definitive -> 1, Simple -> 1, Processing -> 1, The -> 1, Spark -> 1, Made -> 1, Guide -> 1, Big -> 1, : -> 1, Data -> 1)
words.countByValueApprox(400,0.95)

words.first()
//其实就是运行了reduce函数
words.max()
words.min()
words.take(10)

//保存文件
/*因为RDD是分布式的集合，必须将RDD写入到本地才能加载到其他的数据源中。
必须遍历分区将每个分区的内容保存到本地，再保存到其他数据源中。
spark会将RDD中每个份额去读读取出来，并写到指定的位置中*/

//words.saveAsTextFile("file:/tmp/bookTitle")
//如果需要对文件进行压缩,就设置压缩编码器
//这方法用不到，无办法加载重载。没有这个库啊，还是没升级？
//words.saveAsTextFile("file:/tmp/bookTitleCompressed",classOf[BZip2Codec])

//二进制文件，对象文件
//怎么知道我是一个序列文件？？
//Save this RDD as a SequenceFile of serialized objects.
//亲儿子啊序列文件
//将此RDD保存为序列化对象的SequenceFile
//words.saveAsObjectFile("tmp/my/sequenceFilePath")

//hadoopFile
//Hapoop生态的文件

//缓存
words.cache()
words.getStorageLevel

//检查点。
//缓存和检查点其实是有区别的，这本书没有详细的讲，另外一本书有详细讲
//hdfs目录，不要使用本地文件系统
spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")

//使用pipe方法调用系统命令操作RDD
/*pipe方法可能是spark最有趣的方法之一，通过pipe方法，可以利用管道技术调用外部进程来生成RDD。将每个数据分区交给指定的外部进程来计算得到结果RDD，
每个分区的所有元素被当成做另一个外部进程的标准输入，输入元素由换行符分隔。最终结果由该外部进程的标准输入，输入元素由换行符分隔。最终结果由该外部
进程的标准输出生成，标准输出的每一行产生输出分区的一个元素。空分区也会调用一个外部进程。*/

words.pipe("wc -l").collect()
//wc 是管道命令，输入是words的分区数据，输出就是转换后的分区数据。
//结果是获取到每个分区的行数。

//words.map()
//这里是输入分区迭代器，输出分区迭代器，这里实现的是一个分区输出一个元素为1的迭代器。
words.mapPartitions(part => Iterator[Int](1)).sum()

//words.mapPartitions(pait => pait.hasNext)

//foreachPartition
words.foreachPartition { iter =>

  import java.io._
  import scala.util.Random
  val randomFileName = new Random().nextInt()
  val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))

  //写入文件中
  while (iter.hasNext)
  {
    pw.write(iter.next())
  }

  pw.close()

}

//glom
//把每个分区的数据都收集成为一个数组。数组类型的RDD
//RDD[Array[T]]
spark.sparkContext.parallelize(Seq("hello","world"),2).glom().collect()


