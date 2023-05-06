import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

object sparkJob {


  def main(args: Array[String]): Unit = {


    println("运行主方法了啊")

    val spark = SparkSession.builder()
      .appName("spark job")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

      spark.sql("show databases").show();

      new SparkConf()



  }

}
