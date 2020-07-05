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

object JMSSparkStream {

    def functionToCreateContext():StreamingContext = {
         
         val checkpointDirectory = "hdfs://HDPHAPR/user/cbhdj/spark/checkpoint"
        val sparkConf = new SparkConf().setAppName("testReceiver").set("spark.streaming.receiver.writeAheadLog.enable", "true")
        //sc.stop
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(10))
        ssc.checkpoint(checkpointDirectory)

        val converter: Message => Option[String] = {
            case msg: TextMessage =>
            Some(msg.getText)
        case _ =>
            None
        }
        var jndiProperties = new Properties()

        //TODO: defining variable. need to pass as parameter or in properties files

        val JNDI_initial_Context_Factory_Class = "com.tibco.tibjms.naming.TibjmsInitialContextFactory"
        val JNDI_Provided_URL = "tcp://jndi_t1.nscorp.com:10060,tcp://jndi_t2.nscorp.com:10060"
        val JNDI_Principal = "jndi"
        val JNDI_Credentials = "jndi"
        val JNDI_connection_Factory_name = "PersistentSyncFT01TCF"
        val user = "sytepbdt"
        val credentials = "nikita14"
        val destinationInfo = "NSCORP.BIGDATA.BIGDATAGATEWAY.PTCASSETEVENTNIFI.RECEIVE.XML.V1"
        val JMS_Queue = "NSCORP.BIGDATA.BIGDATAGATEWAY.PTCASSETEVENTNIFI.RECEIVE.XML.V1"

        jndiProperties.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_initial_Context_Factory_Class);
        jndiProperties.put(Context.PROVIDER_URL, JNDI_Provided_URL);
        jndiProperties.put(Context.SECURITY_PRINCIPAL, JNDI_Principal);
        jndiProperties.put(Context.SECURITY_CREDENTIALS, JNDI_Credentials);

        val msgs = JmsStreamUtils.createSynchronousJmsQueueStream(ssc,
            JndiMessageConsumerFactory(jndiProperties, QueueJmsDestinationInfo(JMS_Queue), JNDI_connection_Factory_name),
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

            // rdd.saveAsTextFile("hdfs:///...-"+Calendar.getInstance().getTimeInMillis())
            } else {
            println("rdd is empty")
            }
        })
        ssc
        }

    def main(args: Array[String]): Unit = {
        val checkpointDirectory = "hdfs://HDPHAPR/user/cbhdj/spark/checkpoint"
        val streamC = StreamingContext.getOrCreate(checkpointDirectory, () => functionToCreateContext() )

        streamC.start()
        streamC.awaitTermination()

    }
}
