import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

object start {

  def main(args: Array[String]): Unit = {

    val builder:Builder = SparkSession.builder()
    builder.appName("Spark example").master("local[*]")
    builder.enableHiveSupport()
    val  spark : SparkSession =  builder.getOrCreate()
    val sc :SparkContext =  spark.sparkContext
    sc.setLogLevel("WARN")

  /*  System.out.printf(sc.getConf.get("spark.sql.warehouse.dir")) */
  /* spark.sql("create database test")
    spark.sql("use test")
    //这样create table 不是临时表 isTemporary=false
    spark.sql("create table test.t1")*/


    spark.sql("show databases").show()
    spark.catalog.listDatabases().show()
    spark.sql("show tables in test").show()
    spark.sql("show tables in default").show()

    //这样创建的是外部表，数据在warehouse外部，删除表不会删除外部的数据，在spark内成为非托管表
/*    spark.sql("CREATE TABLE test.flights (" +
      "  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)" +
      "USING JSON OPTIONS (path 'E:/workFile/Spark-book/data/flight-data/json/2015-summary.json')").show()*/

    //这样创建出来的是临时表  isTemporary=true;
   /* spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")*/

   /* spark.sql("select * from flights").show()
    spark.sql("describe flights").show(false)
    spark.sql("show create table flights").show(false)*/


   /* spark.sql("describe default.flightsdf").show(false)
    spark.sql("show create table default.flightsdf").show(false)*/

    //这样会把数据移动到warehouse下面，是一个托管表？ hive内部表 ,物理数据在warehouse内部，在spark上称作是托管表，删除数据会删除warehouse上面的数据
    /*val flightDF =   spark.read.json("data/flight-data/json/2015-summary.json")
    flightDF.write.saveAsTable("flightsDF")*/

/*  spark.sql("drop table default.flightsdf")
    spark.sql("drop table default.flights")*/

  /*  spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")

    spark.sql("create table flights_select as select * from some_sql_view")*/

    /*spark.sql("describe default.flights_select").show(false)
    spark.sql("show create table default.flights_select").show(false)*/

    spark.sql("SELECT 1 + 1").show()

    // COMMAND ----------
    // in Scala
    spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view") // DF => SQL

    spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count)
    FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
    """)
      .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
      .count() // SQL => DF


    spark.sql("show databases").show()
    spark.sql("show tables in test").show()
    spark.sql("show tables in default").show()
  }

}
