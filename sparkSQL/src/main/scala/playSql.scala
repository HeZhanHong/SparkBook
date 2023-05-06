import org.apache.spark.sql.SparkSession

object playSql {

  /**
   * 这样创建的是外部表，数据在warehouse外部，删除表不会删除外部的数据，在spark内成为非托管表
   * @param spark
   */
  def play (spark : SparkSession): Unit =
  {
    spark.sql("CREATE TABLE test.flights (" +
      "  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)" +
      "USING JSON OPTIONS (path 'data/flight-data/json/2015-summary.json')").show(false)
  }

  /**
   * 这样创建出来的是临时表  isTemporary=true;
   * @param spark
   */
  def play2 (spark : SparkSession): Unit =
  {
    spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")

    spark.sql("select * from some_sql_view").show(false)
  }

  /**
   * 这样会把数据移动到warehouse下面，是一个托管表，hive内部表 ,物理数据在warehouse内部，
   * 在spark上称作是托管表，删除数据会删除warehouse上面的数据
   * @param spark
   */
  def play3 (spark : SparkSession): Unit =
  {
    val flightDF =   spark.read.json("data/flight-data/json/2015-summary.json")
    flightDF.write.saveAsTable("flightsDF")

    spark.sql("select * from flightsDF").show(false)
  }

  /**
   * 这样可以把临时表，转换成非临时表，而且还是托管表
   * @param spark
   */
  def play4 (spark : SparkSession): Unit =
  {
    spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")
    spark.sql("create table flights_select as select * from some_sql_view").show(false)
  }

  /**
   * 使用DF转换成使用sql，再用sql转换成DF，中间表其实就是临时表来的
   * @param spark
   */
  def play5 (spark : SparkSession): Unit =
  {
    spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view") // DF => SQL

    spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count)
    FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
    """)
      .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
      .show(false)
    // SQL => DF
  }


  /**
   * 读取外部的json数据，创建外部表
   * @param spark
   */
  def play6 (spark : SparkSession): Unit =
  {
    spark.sql("""CREATE TABLE test.flights_json (
                |  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |USING JSON OPTIONS (path 'data/flight-data/json/2015-summary.json')""".stripMargin)
  }

  /**
   * 读取外部的csv数据，创建外部表
   * @param spark
   */
  def play7 (spark : SparkSession): Unit =
  {
    spark.sql("""CREATE TABLE test.flights_csv (
                |  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |USING csv OPTIONS (path 'data/flight-data/csv/2015-summary.csv')""".stripMargin)
  }

  /**
   * 创建的是内部表，存储的文件格式为parquet
   * @param spark
   */
  def play8 (spark : SparkSession): Unit =
  {
    spark.sql(
      """create table if not exists test.flights_from_select using parquet as
        |select * from test.flights
        |""".stripMargin)
  }

  /**
   * 创建分区表，内部表
   * @param spark
   */
  def play9 (spark : SparkSession): Unit =
  {
    spark.sql(
      """create table if not exists test.partitioned_flights using parquet partitioned by (DEST_COUNTRY_NAME)
        |as
        |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM test.flights
        |""".stripMargin)
  }

  /**
   * 使用hive的语法创建了外部表啊
   * @param spark
   */
  def play10 (spark : SparkSession): Unit =
  {
    spark.sql(
      """
        |create external table  if not exists  test.hive_flights(
        |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'E:\\workFile\\Spark-book\\data\\flight-data-hive\\'
        |""".stripMargin)
  }


  /**
   * 从select语句创建表，生成的数据会把LOCATION路径的数据覆盖的。
   * @param spark
   */
  def play11 (spark : SparkSession): Unit =
  {
    spark.sql(
      """
        |CREATE EXTERNAL TABLE if not exists test.hive_flights_2
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |LOCATION 'E:\\workFile\\Spark-book\\data\\flight-data-hive\\' AS SELECT * FROM test.flights
        |""".stripMargin)
  }


  /**
   * 输出所有Table
   * @param spark
   */
  def showTables (spark : SparkSession): Unit =
  {
    spark.sql("show databases").show()
    spark.sql("show tables in default").show()
    spark.sql("show tables in test").show()
  }

  /**
   *输出数据表的详细信息
   * @param spark
   * @param tableName
   */
  def  descTable (spark : SparkSession,tableName :String): Unit =
  {
    spark.sql(s"describe $tableName").show(false)
    spark.sql(s"show create table $tableName").show(false)
  }

  /**
   * 输出表所有行
   * @param spark
   * @param tableName
   */
  def selectAll (spark : SparkSession,tableName :String): Unit =
  {
    spark.sql(s"select * from $tableName").show(1000,false)
    println("总行数 :" + spark.sql(s"select * from $tableName").count())
  }





}
