import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import playSql._

object TestSparkSql {

  def main(args: Array[String]): Unit = {

    val builder:Builder = SparkSession.builder()
    builder.appName("Spark example").master("local[*]")
    builder.enableHiveSupport()
    val  spark : SparkSession =  builder.getOrCreate()
    val sc :SparkContext =  spark.sparkContext
    sc.setLogLevel("WARN")

    //showTables(spark)

 /*   play6(spark)
    play7(spark)*/

    //play11(spark)

    //descTable(spark,"test.hive_flights_2")

    selectAll(spark,"test.hive_flights")
    selectAll(spark,"test.hive_flights_2")


    showTables(spark)
  }



}
