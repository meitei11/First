import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object parquet {

  def main(args: Array[String]) {
    val inputFile = "src/resources/test2.json"
    val outputFile = "src/resources/parquet/test2"
    val conf:SparkConf = new SparkConf().setSparkHome("/usr/local/spark")
      .setAppName("First App").setMaster("local")

    val sc:SparkContext = new SparkContext(conf)
    sc.


    val spark = SparkSession
      .builder()
      .appName("First")
      .config(conf)
      .getOrCreate()

    val schema = new StructType().add("field1", StringType, true)
        .add("field2", IntegerType, true)


    convertToParquet(spark, schema, inputFile, outputFile)
    spark.stop()
  }

  def convertToParquet(spark:SparkSession, schema: StructType, inputFile:String, outputFile:String)= {
    println("===================================================================================")
    println(s"Reading JSON from: $inputFile and writing to: $outputFile ")
    val df = spark.read.schema(schema).json(inputFile)
    df.printSchema()
    println("===================================================================================")
    //df.write.parquet(outputFile)
    println("===================================================================================")
    val newDF = spark.read.parquet("src/resources/parquet/*")
    newDF.show()
    println("===================================================================================")
  }

}