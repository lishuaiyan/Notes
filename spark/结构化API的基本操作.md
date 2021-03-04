# 结构化API的基本操作

* 数据源

```scala
/**
* 当使用Spark进行生产级别的ETL的时候， 最好采用显式定义Schema的方式，尤其在处理诸如CSV和JSON之类
* 的无类型数据源时更是如此，因为模式推断方法会根据读入数据类型而变化
*/
val peopleDF = spark.read("examples/src/main/resources/people.json")
```

* 列与表达式


  列的引用方式 `col` / `column` /` $"myColumn"`/ `'myColumn` ,后两种为 scala api的独有的，一般习惯使用 `col`

  表达式是对一个DataFrame中某一个记录的一个或多个值的一组转换操作

    * 列只是表达式 `expr("col")` 等同于 `col("col"`

	* 列与对这些列的转换操作被编译后生成的逻辑计划， 与解析后的表达式的逻辑计划是一样的

	* SQL 与 DataFrame代码在执行之前会编译成相同的底层逻辑树，这意味着SQL表达式与DataFrame代码的性是一样的
