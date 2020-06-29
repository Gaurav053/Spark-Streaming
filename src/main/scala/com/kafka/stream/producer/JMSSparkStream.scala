package com.kafka.stream.producer

import java.net.ServerSocket
import java.util.{Properties, UUID}
import javax.jms._
import javax.naming.{Context, InitialContext}
import java.util.Hashtable
import org.apache.spark.streaming.jms._
import net.tbfe.spark.streaming.jms._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Minutes, Seconds}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import java.util.Calendar
import scala.concurrent.duration._

// JSM Queue
// userid
// password
// JNDI Initial Context Factory Class
// JNDI Provided URL
// JNDI NNAMe of the connection Factory
// JNDI/JMS Client Libraries
// JNDI Principal
// JNDI Credentials

object JMSSparkStream {
{
  // def functionToCreateContext(host: String, port: String, qm: String, qn: String, checkpointDirectory: String): StreamingContext = {
  def functionToCreateContext(JMS_Queue: String, userid: String, password: String, JNDI_initial_Context_Factory_Class: String,
                              JNDI_Provided_URL: String, JNDI_connection_Factory_name: String, JNDI_JMS_Client_lib: String, JNDI_Principal: String,
                              JNDI_Credentials: String, checkpointDirectory: String, destinationInfo: String): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("testReceiver").set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint(checkpointDirectory)

    val converter: Message => Option[String] = {
      case msg: TextMessage =>
        Some(msg.getText)
      case _ =>
        None
    }


    // case class JndiMessageConsumerFactory(jndiProperties: Properties,
    //   destinationInfo: JmsDestinationInfo,
    //   connectionFactoryName: String = "ConnectionFactory",
    //   messageSelector: String = "")
    //   extends MessageConsumerFactory with Logging {

    // case class MQConsumerFactory(mqHost: String,
    //                     mqPort: Int,
    //                     mqQmgr: String,
    //                     mqQname: String,
    //                     mqUser: String,
    //                     mqCred: String,
    //                     mqChannel: String,
    //                     connectionFactoryName: String = "ConnectionFactory",
    //                     messageSelector: String = "")

    // val msgs = JmsStreamUtils.createSynchronousJmsQueueStream(ssc, MQConsumerFactory(host, port.toInt, qm, qn, user, credentials),


    Hashtable<String, String> jndiProperties = new Hashtable<String, String>();
    jndiProperties.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_initial_Context_Factory_Class);
    jndiProperties.put(Context.PROVIDER_URL, JNDI_initial_Context_Factory_Class);
    jndiProperties.put(Context.SECURITY_PRINCIPAL, JNDI_Principal);
    jndiProperties.put(Context.SECURITY_CREDENTIALS, JNDI_Credentials);



    val msgs = JmsStreamUtils.createSynchronousJmsQueueStream(ssc, JmsStreamUtils.JndiMessageConsumerFactory(jndiProperties, destinationInfo, JNDI_connection_Factory_name, user, credentials),

      converter,
      1000,
      1.second,
      10.seconds,
      StorageLevel.MEMORY_AND_DISK_SER_2
    )

    msgs.foreachRDD ( rdd => {
      if (!rdd.partitions.isEmpty){
        // This is where you process the messages received
        println("messages received:")
        rdd.foreach(println)
        // You can save the collection of messages to HDFS (or alternatively locally by specifying "file:///")
        // The timestamp was added here to ensure unique folder ids
        rdd.saveAsTextFile("hdfs:///...-"+Calendar.getInstance().getTimeInMillis())
      } else {
        println("rdd is empty")
      }
    })

    ssc
  }

  //

  def main(args: Array[String]): Unit = {
    if (args.length < 9){
      // System.err.println("Usage: <host> <port> <queue manager> <queue name>")
      // System.err.println("Usage: <JSM Queue> <port> <queue manager> <queue name>")
      // Fix the message
      System.exit(1)
    }

    val Array(host, port, qm, qn) = args

    // Specify the path for the checkpoint directory can be local file or hdfs
    // This is used to recover messages if the application fails
    val checkpointDirectory = "hdfs://user/cbhdj/spark/checkpoint"

    // Get StreamingContext from checkpoint data or create a new one
    val streamC = StreamingContext.getOrCreate(checkpointDirectory, () => functionToCreateContext(host, port, qm, qn, checkpointDirectory))

    streamC.start()
    streamC.awaitTermination()
  }
}

