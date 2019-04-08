package org.jfratzke.spark.paradigm


import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * A simple paradigm for large complicated Spark Pipelines. It is often the case in Data Engineering where you have
  * one long single complicated pipeline for a particular dataset used in a business case. This approach provides a means
  * to split each DF manipulation into logical steps, provide some janitorial service in between steps, and provide a break
  * between large map manipulations that may cause memory issues in Spark. (It is often the case where a lot of operations
  * may be performed without a shuffle task)
  */
abstract class DataFrameRule {

  import DataFrameRule._

  def name: String

  def run(input: DataFrame): DataFrame

  // We can also make this a wrapper for a set of rules rather than just an implementation
  def alternativeRun(input: DataFrame) = {
    val t = List[DataFrame => DataFrame](
      run,
      resize,
      ensureNoDuplicates,
      writeRuleResults
    )
    t.foldLeft(input){ case (acc, rule) => rule(acc) }
  }

  // The beauty here is we can write any # of intermediate steps to check the status of each stage, counts, sanity checks, etc
  def writeRuleResults(res: DataFrame) = {
    res.write.parquet(
      res.sqlContext.getConf("spark.intermediate.basepath") + "/" +
        res.sqlContext.getConf("spark.rundate") + "/" +
        name.toLowerCase.replaceAll(" ", "")
    )
    res
  }

  def resize(res: DataFrame) = res.repartition(PARTITION_SIZE)

  def ensureNoDuplicates(res:DataFrame): DataFrame =
    if (res.groupBy("primary_key").count.where(col("count").gt(1)).count > 0)
      throw new DataFrameException(s"$name failed duplicate count post processing")
    else
      res
}

class DataFrameException(msg: String) extends Exception

object DataFrameRule {

  private val conf = ConfigFactory.load()
  private val PARTITION_SIZE: Int = conf.getInt("partition-size")
  private var OUTPUT_COLUMNS = List[String]()

}

object ExampleRule1 extends DataFrameRule {
  override def name: String = "Example_Rule_1"
  override def run(input: DataFrame): DataFrame = input
}

object ExampleRule2 extends DataFrameRule {
  override def name: String = "Example_Rule_2"
  override def run(input: DataFrame): DataFrame = input
}


object RuleBasedExample {

  protected val sConf: SparkConf = new SparkConf()
    .setAppName("SampleJob")
  protected val spark: SparkSession = new SparkSession.Builder().config(sConf).getOrCreate()

  val RULES: List[DataFrameRule] = List[DataFrameRule](
    ExampleRule1,
    ExampleRule2
  )

  def main(args: Array[String]) = {
    val test = spark.read.load("/Sample/Test")
    val results = RULES.foldLeft(test){ case (acc, rule) =>
      rule.run(acc)
    }
    results.write.save("/output")
  }

}