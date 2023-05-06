import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession.Builder

class A_Gentle_Introduction_to_Spark_Chapter_2_A_Gentle_Introduction_to_Spark {


  def main(args: Array[String]): Unit = {
    val builder:Builder = SparkSession.builder()
    builder.appName("Spark example").master("local[*]")

    val  spark : SparkSession =  builder.getOrCreate()
    val sc :SparkContext =  spark.sparkContext

    val myRange: DataFrame =  spark.range(1000).toDF("number")

    val divisBy2 = myRange.where("number % 2 = 0")

    divisBy2.count()
    divisBy2.show()
  }





}
