import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Assignment {

  def main(args: Array[String]): Unit = {

    // Creating spark session
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkAssignment.prodapt")
      .getOrCreate()

    if(args.length<2){
      System.err.println("Not enough Arguments")
      System.exit(1)
    }

    //input and output directories
    val input_path = args(0)
    val out_path = args(1)

    //Creating Streaming from input
    val inputdf = spark.readStream
      .format("text")
      .load(input_path)

    // Extracting group name from input
    val  parsed_df= inputdf.filter(col("value").contains("omwssu")).withColumn("timeStamp",regexp_extract(col("value"),"""^\{"message":\s"[^\s]+[\s]+([\d|-]+\s+[\d|:]+)""",1)).
      withColumn("message",regexp_extract(  col("value"),"""^.*(http:[^\s]+)""",1)).
      withColumn("cpe_id",regexp_extract(col("message"),"""^.*http:[\S].{1}[^\/]+\/[\w]+\/([^\/]+)""",1)).
      withColumn("fqdn",regexp_extract(col("message"),"""^.*(http:[\S]+errors)""",1)).
      withColumn("action",regexp_extract(col("message"),"""^.*http:[\S].{1}[^\/]+\/[\w]+\/[^\/]+\/([^\/]+)""",1)).
      withColumn("err_code",regexp_extract(col("message"),"""^.*http:[\S].{1}[^\/]+\/[\w]+\/[^\/]+\/[^\/]+\/([\d]\/[\d])""",1))

    val finaldf = parsed_df.withColumn("timeStamp",to_timestamp(regexp_replace(col("timeStamp"),"""[\s]+"""," "))).
      withColumn("err_code",regexp_replace(col("err_code"),"""\/""",".").cast("double")).na.drop("any")

    // streaming extracted group name to output file in json
    finaldf.select(col("fqdn"),col("cpe_id"),col("action"),col("err_code"),col("message"),concat(date_format(col("timeStamp"),"yyyy:MM:dd"),lit("T"),date_format(col("timeStamp"),"HH:mm:ss"),lit("Z")).as("timestamp")).toJSON.
      writeStream.outputMode("append").format("text").option("path",out_path).option("checkpointLocation",out_path).start().awaitTermination()

  }
}
