import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

class partitions {

  def main(args: Array[String]): Unit = {


    val builder:Builder = SparkSession.builder()
    builder.appName("Spark example").master("local[*]")

    val  spark : SparkSession =  builder.getOrCreate()
    val sc :SparkContext =  spark.sparkContext
    sc.setLogLevel("WARN")


    //file:/D:/BigDataSoft/IntelliJ IDEA 2021.2.3/jbr/bin/data/retail-data/all
    val df = spark.read.option("header","true").option("inferSchema","true").csv("data/retail-data/all/")
    val rdd = df.coalesce(10).rdd
    rdd.collect()

    //俩个默认内置分区器，HashPartitioner和RangePartitioner（根据数值范围分区）
    //离散值就用hash，连续值就用Range

    import org.apache.spark.HashPartitioner
    rdd.map(r => r(6)).take(5).foreach(println)
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)



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



  }

}
