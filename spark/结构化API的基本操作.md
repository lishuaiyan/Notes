# 结构化API的基本操作

###  数据源

```scala
/**
* 当使用Spark进行生产级别的ETL的时候， 最好采用显式定义Schema的方式，尤其在处理诸如CSV和JSON之类
* 的无类型数据源时更是如此，因为模式推断方法会根据读入数据类型而变化
*/
val peopleDF = spark.read("examples/src/main/resources/people.json")
```

###  列与表达式


  列的引用方式 `col` / `column` /` $"myColumn"`/ `'myColumn` ,后两种为 scala api的独有的，一般习惯使用 `col`

  表达式是对一个DataFrame中某一个记录的一个或多个值的一组转换操作

    * 列只是表达式 `expr("col")` 等同于 `col("col"`

	* 列与对这些列的转换操作被编译后生成的逻辑计划， 与解析后的表达式的逻辑计划是一样的

	* SQL 与 DataFrame代码在执行之前会编译成相同的底层逻辑树，这意味着SQL表达式与DataFrame代码的性是一样的


###  常用操作

* 创建DataFrame

```scala
// 创建行
import org.apache.spark.sql.Row
val myRow = Row("Hello", null, 1, false)

/*
* 创建DataFrame
*/

// 通过读取数据文件创建DataFrame
val df = spark.read.format("json").load("/data/summary.json")
df.createOrReplaceTempView("dfTable")

// 通过行创建DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema = new StructType(Array(
	new StructField("some", StringType, true),
	new StructField("col", StringType, true),
	new StructField("names", LongType, false)
))
val myRows = Seq(Row("Hello", null, 1L))
val MyRdd = spark.sparkContext.parallelize(myRows)

val myDf = spark.createDataFrame(myRdd, myManualSchema)
myDf.show()

```

```py
# 创建行
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

# 创建DataFrame

# 通过读取数据文件创建DataFrame
df = spark.read.format("json").load("/data/summary.json")
df.createOrReplaceTempView("dfTable")

# 通过行创建DataFrame
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
	StructField("some", StringType(), True),
	StructField("col", StringType(), True),
	StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()

```

* select 和 selectExpr函数

```scala
// select示例
import org.apache.spark.sql.functions.{expr, col, column}

df.select(
	df.col("DEST_COUNTRY_NAME"),
	col("DEST_COUNTRY_NAME"),
	column("DEST_COUNTRY_NAME"),
	'DEST_COUNTRY_NAME,
	$"DEST_COUNTRY_NAME",
	expr("DEST_COUNTRY_NAME")
).show(2)

// selectExpr示例

// 字段重命名
df.selectExpr("DEST_COUNTRY_NAME as newColumn", "DEST_COUNTRY_NAME").show(2)

df.selectExpr(
	// 包含原始表的所有字段
	"*",
	// 增加一个新列
	"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
	.show(2)

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))".show(2)


```

```py
# select示例
from pyspark.sql.functions import expr, col, column

df.select(
	expr("DEST_COUNTRY_NAME"),
	col("DEST_COUNTRY_NAME"),
	column("DEST_COUNTRY_NAME"))\
	.show(2)

# selectExpr 示例

# 字段重命名
df.selectExpr("DEST_COUNTRY_NAME as newColumn", "DEST_COUNTRY_NAME").show(2)

df.selectExpr(
	# 包含原始表中所有列
	"*", 
	# 增加一个新列
	"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
		.show(2)
)

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
```

* 转换操作成Spark类型(字面量)

```scala
import org.apache.spark.sql.functions.lit
df.select(expr("*"), lit(1).as("One")).show(2)
```

```py
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)
```
* 添加列

```scala
df.withColumn("number", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
.show(2)
```

```py
df.withColumn("numberOne", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
.show(2)
```
* 重命名列

```scala
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
```

```py
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
```

* 保留字与关键字

      遇到列名中包含空格或者连字符等保留字符，此时需要通过反引号 (`) 来引用

* 区分大小写

      Spark默认是不区分大小写的，但可以通过如下配置使Spark区分大小写

```sql
-- in SQL
set spark.sql.caseSensitive true
```

* 更改列的类型

```scala
df.withColumn("count2", col("count").cast("long"))
```

* 过滤行

      以下过滤器在scala和python中是等效的

```scala
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
```

    Spark会同时执行所有的过滤操作，所以只需要按照先后顺序以链接的方式把这些条件串联起来，然后让Spark执行剩下的工作

```scala
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
.show(2)
```

```py
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
.show(2)
```


* 随机抽样

```scala
val seed = 5
// TRUE 为有放回的抽样，FALSE 为无放回的抽样
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement, fraction, seed).count()
```

```py
seed = 5
// TRUE 为有放回的抽样，FALSE 为无放回的抽样
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()
```

* 随机分割

      通常是在机器学习算法中，用于分割数据集来创建数据集，验证集和测试集
      由于随机分割是一种随机方法，所以我们还需要指定一个随机种子 seed

```scala
val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
dataFrames(0).count() > dataFrames(1).count() // 结果为False
```

```py
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # 结果为False
```


* 连接和追加行（联合操作）

  * 想联合两个DataFrame，你必须确保它们具有相同的模式和列数，否则联合操作将失败，因为目前联合操作是基于位置而不是基于数据模式Schema执行

```scala
import org.apache.spark.sql.Row

val schema = df.schema
val newRows = Seq(
	Row("New Country", "other country", 5L),
	Row("New Country 2", "other Country 2"， 1L)
)
val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDf = spark.createDataFrame(parallelizedRows, schema)
df.union(newDf)
.where("count = 1")
.where($"ORIGIN_COUNTRY_NAME" =!= "UNited States")
.show()
```

```py
from pyspark.sql import Row

schema = df.schema
newRows = [
	Row("New COuntry", "other Country", 5L),
	Row("New Country 2", "other Country 2", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDf = spark.createDataFrame(parallelizedRows, schema)
df.union(newDf)\
.where("count = 1")\
.where(col("ORIGIN_COUNTRY_NAME" != "United States"))\
.show()
```

* 行排序

      sort 和 orderBy 是相互等价的操作
	  一个高级功能就是可以指定空值在排序列表中的位置
	  1、asc_nulls_first 指示空值安排在升序排列前面
	  2、asc_nulls_last 指示空值安排在升序排列的后面
	  3、desc_nulls_first 和 desc_nulls_last 同理

```scala
import org.spark.sql.functions.{desc, asc}
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

df.orderBy(expr("count desc")).show(2)
df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

// 处于性能考虑，可以使用sortWithinPartitions方法对每个分区内部进行排序
spark.read.format("json").load("-summary.json")
.sortWithinPartitions("count")
```

```py
# in Python
from pyspark.sql.functions import desc, asc
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# 处于性能考虑，可以使用sortWithinPartitions方法对每个分区内部进行排序
spark.read.format("json").load("-summary.json")\
.sortWithinPartitions("count")
```

* 重划分和合并

```scala
df.rdd.getNumPartitions
// 重分区会导致全面洗牌
df.repartition(5)
// 如果你知道经常按某一列执行过滤操作，则根据该列进行重分区是很有必要的
df.repartition(col("count"))
df.repartition(5, col("count"))
// 合并操作，不会导致数据的全面洗牌，但会尝试合并分区
df.coalesce(2)
```


```py
# in Python
df.rdd.getNumPartitions
# 重分区会导致全面洗牌
df.repartition(5)
# 如果你知道经常按某一列执行过滤操作，则根据该列进行重分区是很有必要的
df.repartition(col("count"))
df.repartition(5, col("count"))
# 合并操作，不会导致数据的全面洗牌，但会尝试合并分区
df.coalesce(2)
```
