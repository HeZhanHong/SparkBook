import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}

val builder:Builder = SparkSession.builder()
builder.appName("Spark example").master("local[*]")

val  spark : SparkSession =  builder.getOrCreate()
val sc :SparkContext =  spark.sparkContext
sc.setLogLevel("WARN")

//连接操作

//创造实验数据

import spark.implicits._

val person = Seq(
  (0, "Bill Chambers", 0, Seq(100)),
  (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
  (2, "Michael Armbrust", 1, Seq(250, 100)))
  .toDF("id", "name", "graduate_program", "spark_status")
val graduateProgram = Seq(
  (0, "Masters", "School of Information", "UC Berkeley"),
  (2, "Masters", "EECS", "UC Berkeley"),
  (1, "Ph.D.", "EECS", "UC Berkeley"))
  .toDF("id", "degree", "department", "school")
val sparkStatus = Seq(
  (500, "Vice President"),
  (250, "PMC Member"),
  (100, "Contributor"))
  .toDF("id", "status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

val wrongJoinExxpression = person.col("name") === graduateProgram.col("school")

person.join(graduateProgram,joinExpression).show()

spark.sql("select * from person join graduateProgram on person.graduate_program = graduateProgram.id")


//设置joinTyoe
/*case "inner" => Inner
case "outer" | "full" | "fullouter" => FullOuter
case "leftouter" | "left" => LeftOuter
case "rightouter" | "right" => RightOuter
case "leftsemi" => LeftSemi
case "leftanti" => LeftAnti
case "cross" => Cross*/

var joinType = "inner"
person.join(graduateProgram,joinExpression,joinType).show()


//外连接
joinType = "outer"
person.join(graduateProgram,joinExpression,joinType).show()

//左外连接
joinType = "leftouter"
person.join(graduateProgram,joinExpression,joinType).show()

//右外连接
joinType = "RightOuter"
person.join(graduateProgram,joinExpression,joinType).show()

//左半连接
joinType = "leftsemi"
person.join(graduateProgram,joinExpression,joinType).show()


//左反连接
joinType = "leftanti"
person.join(graduateProgram,joinExpression,joinType).show()


//自然连接
//不必指定条件，隐性猜测，就是人工智障

//笛卡尔连接，交叉连接
joinType = "cross"
person.join(graduateProgram,joinExpression,joinType).show()


//连接操作常见问题与解决方案

//对复杂类型的连接操作
person.withColumnRenamed("id","personId").join(sparkStatus,expr("array_contains(spark_status,id)")).show()

//处理重复的列名
val gadProgramDupe = graduateProgram.withColumnRenamed("id","graduate_Program")

val joinExpr = gadProgramDupe.col("graduate_Program") === person.col("graduate_Program")

person.join(gadProgramDupe,joinExpr).show()

person.join(gadProgramDupe,"graduate_Program").show()


//方法二，连接后删除列
//现在都不会报错了还删个毛

person.join(gadProgramDupe,joinExpr).drop(person.col("graduate_Program")).show()

val joinExpr = person.col("graduate_Program")  === graduateProgram.col("id")

//不删除也不会报错，spark已经优化
person.join(graduateProgram,joinExpr).drop(graduateProgram.col("id")).show()


//方法三，在连接前重命名列



//spark如何进行连接

//all to all shuffle join

//广播连接

//会自动设置也可以显性设置
//自动设置是基于spark的优化决策
val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpr).explain()

/*== Physical Plan ==
  *BroadcastHashJoin [graduate_program#11], [id#27], Inner, BuildRight
:- LocalTableScan [id#9, name#10, graduate_program#11, spark_status#12]
+- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
+- LocalTableScan [id#27, degree#28, department#29, school#30]*/


//显性设置
import org.apache.spark.sql.functions.broadcast
val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

//Marks a DataFrame as small enough for use in broadcast joins.
//The following example marks the right DataFrame for broadcast hash join using joinKey.
person.join(broadcast(graduateProgram), joinExpr).explain()


//sparksql 就是用mapjoin
