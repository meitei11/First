import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}


object parquet {

  def main(args: Array[String]) {
    val conf:SparkConf = new SparkConf()
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

  def convertToParquet(sc:SparkContext, sql:SQLContext, inputFile:String, outputFile:String): Unit ={
    println(s"Reading JSON from: $inputFile and writing to: $outputFile")
    val df = sc.read.json(inputFile)
    df.save(outputFile, "parquet")
  }

}