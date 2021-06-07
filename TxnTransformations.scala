import org.apache.kafka.connect.data.Struct
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.codehaus.commons.compiler.samples.DemoBase.explode
import schema.txnSchema

import scala.math.pow
import scala.Int.int2double
import scala.util.Random

object
TxnTransformations extends App {

  val BOOTSTRAP_SERVER = "localhost:9092"
  val TOPIC_NAME = "txnmessage"
  val HDFS_FILE_PATH = "hdfs://localhost:9000/data/json/txn_raw_data"
  val CHECKPOINT_LOCATION = "hdfs://localhost:9000/data/checkpoint/txn_raw_data"
  val FILTERED_FIELDS = List("user_id","first_name","last_name","gender","city","state"
    ,"zip","email","nationality","tran_card_type","tran_data","product_id","tran_amount","txn_datetime")

  val spark = SparkSession.builder()
    .appName("Txn Transformations")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def readFromKafka(bootstrapServer:String, topic:String):DataFrame={
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapServer)
      .option("subscribe",topic)
      .load()
      .select(col("value").cast("String"))
  }

  def writeToConsole(dataframe:DataFrame)={
    dataframe.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  def persistToHdfs(txnDataframe:DataFrame, hdfsFilePath:String, checkpointPath:String)={
    txnDataframe.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("json")
      .option("path",hdfsFilePath)
      .option("checkpointLocation",checkpointPath)
      .start()
  }

  def transformTxnCassandra(txnDataframe:DataFrame, filtered_fields:List[String]):DataFrame={
    txnDataframe.select(filtered_fields.map(field=>col(field)):_*)
  }

  def transformTxnMongoDB(txnDataframe:DataFrame):Seq[DataFrame]={
    val txnDataframeWithYear = txnDataframe.withColumn("txn_year",year(col("txn_datetime")))
    val txnNationalityCount = txnDataframeWithYear.groupBy(col("nationality")).count()
    val txnCardTypeCount = txnDataframeWithYear.groupBy(col("tran_card_type")).count()
    val txnAmountPerCardType = txnDataframeWithYear.groupBy(col("tran_card_type"))
      .sum("tran_amount")
    val txnAmountPerCardNationality = txnDataframeWithYear.groupBy(col("txn_year")
      ,col("nationality")).sum("tran_amount")
    Seq(txnNationalityCount,txnCardTypeCount,txnAmountPerCardType,txnAmountPerCardNationality)
  }

  val getTxnSrc = udf((input:Seq[String]) =>
    input(Random.nextInt(input.length)))

  val getTxnAmt = udf((amount:Double,randNum:Int) =>
    (Random.nextInt(randNum)*amount) + Random.nextInt(randNum))

  val getProductId = udf((productId:String,codeLength:Int) => {
    val productNumber = Random.nextInt(pow(10, codeLength).toInt).toString
    productId.split("-")(0) + "-" +
      "0" * (pow(10, codeLength).toInt.toString.length - productNumber.length) +
      productNumber
  })
def transformTxn(txnDataframe:DataFrame, txnSchema:StructType):DataFrame={
  txnDataframe.withColumn("data", from_json(col("value"), txnSchema))
  .select("data.*")
    .withColumn("record", functions.explode(col("results")))
    .drop("results")
    .select("*", "record.*").drop("record")
    .select("user.*", "*").drop("user")
    .select("name.*", "location.*", "*","picture.*")
    .drop("name", "location", "picture")
    .select("*", "tran_detail.*").drop("tran_detail")
    .withColumn("tran_card_type", getTxnSrc(col("tran_card_type")))
    .withColumn("tran_amount",getTxnAmt(col("tran_amount"),lit(15)))
    .withColumn("product_id",getProductId(col("product_id"),lit(4)))
    .withColumn("txn_datetime",from_unixtime(col("registered"),"yyyy-MM-dd HH:mm:ss"))

}
   val kafkaDF = readFromKafka(BOOTSTRAP_SERVER,TOPIC_NAME)
  val transformedTxn = transformTxn(kafkaDF,txnSchema)
  persistToHdfs(transformedTxn,HDFS_FILE_PATH,CHECKPOINT_LOCATION)
  val seqDF = transformTxnMongoDB(transformedTxn)
  seqDF.map(df=>writeToConsole(df))






}
