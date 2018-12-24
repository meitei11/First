import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType


object parquet {

  def main(args: Array[String]) {
    val inputFile = "/Users/prabin/work/spark/test.json"
    val outputFile = "/Users/prabin/work/spark/out.parquet"
    val conf:SparkConf = new SparkConf().setSparkHome("/usr/local/spark")
      .setAppName("First App").setMaster("local")
    val spark = SparkSession
      .builder()
      .appName("First")
      .config(conf)
      .getOrCreate()

    val schema = new StructType().add("field1", "string", true)
        .add("field2", "int", true)


    convertToParquet(spark, schema, inputFile, outputFile)
    spark.stop()
  }

  def convertToParquet(spark:SparkSession, schema: StructType, inputFile:String, outputFile:String)= {
    println("===================================================================================")
    println(s"Reading JSON from: $inputFile and writing to: $outputFile ")
    val df = spark.read.schema(schema).json(inputFile)
    df.printSchema()
    println("===================================================================================")

    df.write.parquet(outputFile)
  }

}