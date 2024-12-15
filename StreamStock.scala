import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamStocks {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("stock_metrics"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: StreamStocks <brokers>
                            |  <brokers> is a list of one or more Kafka brokers
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamStocks")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("yanze41_stock_metrics")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stock_metrics_consumer_group",
      "auto.offset.reset" -> "earliest",  // Change from "none" to "earliest" to read all messages
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Parse JSON messages into StockMetrics objects
    val serializedRecords = stream.map(record => {
      println(s"Received Kafka message: ${record.value()}")
      record.value
    })

    val stockReports = serializedRecords.map(rec => {
      try {
        println(s"Attempting to parse JSON: $rec")
        val result = mapper.readValue(rec, classOf[StockMetrics])
        println(s"Successfully parsed StockMetrics: $result")
        result
      } catch {
        case e: Exception =>
          println(s"Error parsing JSON: ${e.getMessage}")
          println(s"Problematic JSON: $rec")
          throw e
      }
    })


    // Store in HBase
    val batchStats = stockReports.map(stock => {
      try {
        println(s"Processing stock record: $stock")

        val dateArr = stock.date.split("-")
        val month = dateArr(0)
        val day = dateArr(1)
        val year = dateArr(2)
        val formattedDate = s"${day}-${month}-${year}"
        val rowKey = s"${stock.symbol}:${formattedDate}"

        println(s"Created row key: $rowKey")

        val put = new Put(Bytes.toBytes(rowKey))

        // Log each column addition
        println("Adding price metrics to HBase")
      put.addColumn(Bytes.toBytes("price"), Bytes.toBytes("daily_open"),
        Bytes.toBytes(stock.`price:daily_open`.toString))
      put.addColumn(Bytes.toBytes("price"), Bytes.toBytes("daily_close"),
        Bytes.toBytes(stock.`price:daily_close`.toString))
      put.addColumn(Bytes.toBytes("price"), Bytes.toBytes("daily_change"),
        Bytes.toBytes(stock.`price:daily_change`.toString))
      put.addColumn(Bytes.toBytes("price"), Bytes.toBytes("return_pct"),
        Bytes.toBytes(stock.`price:return_pct`.toString))
      put.addColumn(Bytes.toBytes("price"), Bytes.toBytes("trading_range"),
        Bytes.toBytes(stock.`price:trading_range`.toString))
      put.addColumn(Bytes.toBytes("price"), Bytes.toBytes("volatility"),
        Bytes.toBytes(stock.`price:volatility`.toString))

      // Volume metrics
      put.addColumn(Bytes.toBytes("volume"), Bytes.toBytes("daily_volume"),
        Bytes.toBytes(stock.`volume:daily_volume`.toString))
      // Calculate price_volume
      val priceVolume = stock.`price:daily_close` * stock.`volume:daily_volume`
      put.addColumn(Bytes.toBytes("volume"), Bytes.toBytes("price_volume"),
        Bytes.toBytes(priceVolume.toString))

      // Trends metrics
      put.addColumn(Bytes.toBytes("trends"), Bytes.toBytes("ma5"),
        Bytes.toBytes(stock.`trends:ma5`.toString))
      put.addColumn(Bytes.toBytes("trends"), Bytes.toBytes("ma10"),
        Bytes.toBytes(stock.`trends:ma10`.toString))
      put.addColumn(Bytes.toBytes("trends"), Bytes.toBytes("ma20"),
        Bytes.toBytes(stock.`trends:ma20`.toString))
      put.addColumn(Bytes.toBytes("trends"), Bytes.toBytes("ma50"),
        Bytes.toBytes(stock.`trends:ma50`.toString))

      table.put(put)
        println(s"Successfully wrote to HBase for key: $rowKey")

        1 // Return 1 for successful processing
      } catch {
        case e: Exception =>
          println(s"Error processing record: ${e.getMessage}")
          throw e
      }
    })
    batchStats.print()  // Print for debugging

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}