import org.apache.spark.sql.SparkSession




object DataFrameExample  extends Serializable {

  def main(args: Array[String]): Unit = {


    println("��ʼ�����������˰�")

    val spark = SparkSession
      .builder()
     // .master("local[*]")
      .appName("Databricks Spark Example")
    //  .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._

    //���﷨�Ҷ�������ѽ���Բۡ�
    spark.udf.register("pointlessUDF", DFUtils.pointlessUDF(_:String):String)
    val authors = Seq("bill,databricks","matei,databricks")
    val authorsDF = spark.sparkContext.parallelize(authors).toDF("raw").selectExpr("split(raw,',') as values")
      .selectExpr("pointlessUDF(values[0]) as name", "values[1] as company")
      .show()

    while (true){

      println("1")
      Thread.sleep(1000)
    }

  }
  object DFUtils extends Serializable {

    def pointlessUDF(raw:String):String={
      raw
    }
  }

}

