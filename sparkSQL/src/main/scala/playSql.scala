import org.apache.spark.sql.SparkSession

object playSql {

  /**
   * �������������ⲿ��������warehouse�ⲿ��ɾ������ɾ���ⲿ�����ݣ���spark�ڳ�Ϊ���йܱ�
   * @param spark
   */
  def play (spark : SparkSession): Unit =
  {
    spark.sql("CREATE TABLE test.flights (" +
      "  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)" +
      "USING JSON OPTIONS (path 'data/flight-data/json/2015-summary.json')").show(false)
  }

  /**
   * ������������������ʱ��  isTemporary=true;
   * @param spark
   */
  def play2 (spark : SparkSession): Unit =
  {
    spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")

    spark.sql("select * from some_sql_view").show(false)
  }

  /**
   * ������������ƶ���warehouse���棬��һ���йܱ�hive�ڲ��� ,����������warehouse�ڲ���
   * ��spark�ϳ������йܱ�ɾ�����ݻ�ɾ��warehouse���������
   * @param spark
   */
  def play3 (spark : SparkSession): Unit =
  {
    val flightDF =   spark.read.json("data/flight-data/json/2015-summary.json")
    flightDF.write.saveAsTable("flightsDF")

    spark.sql("select * from flightsDF").show(false)
  }

  /**
   * �������԰���ʱ��ת���ɷ���ʱ�����һ����йܱ�
   * @param spark
   */
  def play4 (spark : SparkSession): Unit =
  {
    spark.read.json("data/flight-data/json/2015-summary.json")
      .createOrReplaceTempView("some_sql_view")
    spark.sql("create table flights_select as select * from some_sql_view").show(false)
  }

  /**
   * ʹ��DFת����ʹ��sql������sqlת����DF���м����ʵ������ʱ������
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
   * ��ȡ�ⲿ��json���ݣ������ⲿ��
   * @param spark
   */
  def play6 (spark : SparkSession): Unit =
  {
    spark.sql("""CREATE TABLE test.flights_json (
                |  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |USING JSON OPTIONS (path 'data/flight-data/json/2015-summary.json')""".stripMargin)
  }

  /**
   * ��ȡ�ⲿ��csv���ݣ������ⲿ��
   * @param spark
   */
  def play7 (spark : SparkSession): Unit =
  {
    spark.sql("""CREATE TABLE test.flights_csv (
                |  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
                |USING csv OPTIONS (path 'data/flight-data/csv/2015-summary.csv')""".stripMargin)
  }

  /**
   * ���������ڲ����洢���ļ���ʽΪparquet
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
   * �����������ڲ���
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
   * ʹ��hive���﷨�������ⲿ��
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
   * ��select��䴴�������ɵ����ݻ��LOCATION·�������ݸ��ǵġ�
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
   * �������Table
   * @param spark
   */
  def showTables (spark : SparkSession): Unit =
  {
    spark.sql("show databases").show()
    spark.sql("show tables in default").show()
    spark.sql("show tables in test").show()
  }

  /**
   *������ݱ����ϸ��Ϣ
   * @param spark
   * @param tableName
   */
  def  descTable (spark : SparkSession,tableName :String): Unit =
  {
    spark.sql(s"describe $tableName").show(false)
    spark.sql(s"show create table $tableName").show(false)
  }

  /**
   * �����������
   * @param spark
   * @param tableName
   */
  def selectAll (spark : SparkSession,tableName :String): Unit =
  {
    spark.sql(s"select * from $tableName").show(1000,false)
    println("������ :" + spark.sql(s"select * from $tableName").count())
  }





}
